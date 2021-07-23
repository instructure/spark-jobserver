package spark.jobserver.io

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File, ObjectInputStream,
  ObjectOutputStream, StreamCorruptedException}
import java.net.URI
import akka.actor.{ActorRef, Props}
import com.typesafe.config.Config
import org.slf4j.LoggerFactory

import scala.concurrent.duration.DurationInt
import scala.util.{Failure, Success, Try}
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.common.akka.metrics.YammerMetrics
import spark.jobserver.util._
import spark.jobserver.util.DAOMetrics._
import spark.jobserver.JobManagerActor.ContextTerminatedException

import java.time.ZonedDateTime
import scala.concurrent.{Await, Future}

object JobDAOActor {

  val objectLogger = LoggerFactory.getLogger(getClass)

  //Requests
  sealed trait JobDAORequest

  case class SaveBinary(appName: String,
                        binaryType: BinaryType,
                        uploadTime: ZonedDateTime,
                        jarBytes: Array[Byte]) extends JobDAORequest

  case class DeleteBinary(appName: String) extends JobDAORequest

  case class GetApps(typeFilter: Option[BinaryType]) extends JobDAORequest
  case class GetBinaryPath(appName: String,
                           binaryType: BinaryType,
                           uploadTime: ZonedDateTime) extends JobDAORequest

  case class SaveJobInfo(jobInfo: JobInfo) extends JobDAORequest
  case class GetJobInfo(jobId: String) extends JobDAORequest
  case class GetJobInfos(limit: Int, statuses: Option[String] = None) extends JobDAORequest
  case class GetJobInfosByContextId(contextId: String, jobStatuses: Option[Seq[String]]) extends JobDAORequest

  case class SaveJobConfig(jobId: String, jobConfig: Config) extends JobDAORequest
  case class GetJobConfig(jobId: String) extends JobDAORequest
  case class CleanContextJobInfos(contextId: String, endTime: ZonedDateTime)

  case class GetLastBinaryInfo(appName: String) extends JobDAORequest
  case class SaveContextInfo(contextInfo: ContextInfo)  extends JobDAORequest
  case class UpdateContextById(contextId: String, attributes: ContextModifiableAttributes)
  case class GetContextInfo(id: String) extends JobDAORequest
  case class GetContextInfoByName(name: String) extends JobDAORequest
  case class GetContextInfos(limit: Option[Int] = None,
      statuses: Option[Seq[String]] = None) extends JobDAORequest
  case class GetJobsByBinaryName(appName: String, statuses: Option[Seq[String]] = None) extends JobDAORequest
  case class GetBinaryInfosForCp(cp: Seq[String]) extends JobDAORequest

  case class SaveJobResult(jobId: String, result: Any) extends JobDAORequest
  case class GetJobResult(jobId: String) extends JobDAORequest
  case class CleanupJobs(ageInHours: Int) extends JobDAORequest
  case class CleanupContexts(ageInHours: Int) extends JobDAORequest

  //Responses
  sealed trait JobDAOResponse
  case class SaveBinaryResult(outcome: Try[Unit])
  case class DeleteBinaryResult(outcome: Try[Unit])
  case class Apps(apps: Map[String, (BinaryType, ZonedDateTime)]) extends JobDAOResponse
  case class BinaryPath(binPath: String) extends JobDAOResponse
  case class JobInfos(jobInfos: Seq[JobInfo]) extends JobDAOResponse
  case class JobConfig(jobConfig: Option[Config]) extends JobDAOResponse
  case class LastBinaryInfo(lastBinaryInfo: Option[BinaryInfo]) extends JobDAOResponse
  case class ContextResponse(contextInfo: Option[ContextInfo]) extends JobDAOResponse
  case class ContextInfos(contextInfos: Seq[ContextInfo]) extends JobDAOResponse
  case class BinaryInfosForCp(binInfos: Seq[BinaryInfo]) extends JobDAOResponse
  case class BinaryNotFound(name: String) extends JobDAOResponse
  case class GetBinaryInfosForCpFailed(error: Throwable)
  case class JobResult(result: Any) extends JobDAOResponse

  case object InvalidJar extends JobDAOResponse
  case object JarStored extends JobDAOResponse
  case object JobConfigStored
  case object JobConfigStoreFailed

  sealed trait SaveResponse
  case object SavedSuccessfully extends SaveResponse
  case class SaveFailed(error: Throwable) extends SaveResponse

  def props(metadatadao: MetaDataDAO, binarydao: BinaryObjectsDAO,
            config: Config, cleanup: Boolean = false): Props = {
    Props(classOf[JobDAOActor], metadatadao, binarydao, config, cleanup)
  }

  def serialize(value: Any): Array[Byte] = {
    value match {
      case v: Array[Byte] => v // We don't need to do anything, input is already Array[Byte]
      case _ =>
        val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
        val oos = new ObjectOutputStream(stream)
        oos.writeObject(value)
        oos.close()
        stream.toByteArray
    }
  }

  def deserialize(bytes: Array[Byte]): Any = {
    try {
      val ois = new ObjectInputStream(new ByteArrayInputStream(bytes))
      val value = ois.readObject
      ois.close()
      value
    } catch {
      case _: StreamCorruptedException => bytes // Deserializing an object which is already an Array[Byte]
      case ex =>
        objectLogger.error(s"Could not deserialize an object! Exception: ${ex}")
        None
    }
  }
}

class JobDAOActor(metaDataDAO: MetaDataDAO, binaryDAO: BinaryObjectsDAO,
                  config: Config, cleanup: Boolean) extends
  InstrumentedActor with YammerMetrics with FileCacher {
  import JobDAOActor._
  import akka.pattern.pipe
  import context.dispatcher

  if (cleanup) {
    val age = config.getDuration("spark.jobserver.dao-cleanup-after").toHours.toInt
    context.system.scheduler.schedule(2 minutes, 1 hour, self, CleanupContexts(age))
    context.system.scheduler.schedule(2 minutes, 1 hour, self, CleanupJobs(age))
  }

  // Required by FileCacher
  val rootDirPath: String = config.getString(JobserverConfig.DAO_ROOT_DIR_PATH)
  val rootDirFile: File = new File(rootDirPath)

  implicit val daoTimeout = JobserverTimeouts.DAO_DEFAULT_TIMEOUT
  private val defaultAwaitTime = JobserverTimeouts.DAO_DEFAULT_TIMEOUT
  private val jobDAOHelper = new JobDAOActorHelper(metaDataDAO, binaryDAO, config)

  def wrappedReceive: Receive = {
    case SaveBinary(name, binaryType, uploadTime, binaryBytes) =>
      val recipient = sender()
      Future {
        jobDAOHelper.saveBinary(name, binaryType, uploadTime, binaryBytes)
      }.onComplete(recipient ! SaveBinaryResult(_))

    case DeleteBinary(name) =>
      val recipient = sender()
      val deleteResult = DeleteBinaryResult(Try {
        jobDAOHelper.deleteBinary(name)
      })
      recipient ! deleteResult

    case GetApps(typeFilter) =>
      val recipient = sender()
      Utils.timedFuture(binList){
        metaDataDAO.getBinaries.map(
          binaryInfos => binaryInfos.map(info => info.appName -> (info.binaryType, info.uploadTime)).toMap
        )
      }.map(apps => Apps(typeFilter.map(t => apps.filter(_._2._1 == t)).getOrElse(apps))).pipeTo(recipient)

    case GetBinaryPath(name, binaryType, uploadTime) =>
      val recipient = sender()
      val binPath = BinaryPath(jobDAOHelper.getBinaryPath(name, binaryType, uploadTime))
      recipient ! binPath

    case GetJobsByBinaryName(binName, statuses) =>
      Utils.timedFuture(jobQuery){
        metaDataDAO.getJobsByBinaryName(binName, statuses)
      }.map(JobInfos).pipeTo(sender)

    case SaveContextInfo(contextInfo) =>
      saveContextAndRespond(sender, contextInfo)

    case GetContextInfo(id) =>
      Utils.timedFuture(contextRead){
        metaDataDAO.getContext(id)
      }.map(ContextResponse).pipeTo(sender)

    case GetContextInfoByName(name) =>
      Utils.timedFuture(contextQuery){
        metaDataDAO.getContextByName(name)
      }.map(ContextResponse).pipeTo(sender)

    case GetContextInfos(limit, statuses) =>
      Utils.timedFuture(contextList){
        metaDataDAO.getContexts(limit, statuses)
      }.map(ContextInfos).pipeTo(sender)

    case SaveJobInfo(jobInfo) =>
      Utils.timedFuture(jobWrite) {
        metaDataDAO.saveJob(jobInfo)
      }.pipeTo(sender)

    case GetJobInfo(jobId) =>
      Utils.timedFuture(jobRead){
        metaDataDAO.getJob(jobId)
      }.pipeTo(sender)

    case GetJobInfos(limit, statuses) =>
      Utils.timedFuture(jobList){
        metaDataDAO.getJobs(limit, statuses)
      }.map(JobInfos).pipeTo(sender)

    case SaveJobConfig(jobId, jobConfig) =>
      val recipient = sender()
      Utils.usingTimer(configWrite){ () =>
        metaDataDAO.saveJobConfig(jobId, jobConfig)
      }.onComplete {
        case Failure(_) | Success(false) => recipient ! JobConfigStoreFailed
        case Success(true) => recipient ! JobConfigStored
      }

    case GetJobConfig(jobId) =>
      Utils.timedFuture(configRead){
        metaDataDAO.getJobConfig(jobId)
      }.map(JobConfig).pipeTo(sender)

    case GetLastBinaryInfo(name) =>
      Utils.usingTimer(binRead){ () =>
        metaDataDAO.getBinary(name)
      }.map(LastBinaryInfo(_)).pipeTo(sender)

    case CleanContextJobInfos(contextId, endTime) =>
      Utils.timedFuture(jobQuery){
        metaDataDAO.getJobsByContextId(contextId, Some(JobStatus.getNonFinalStates()))
      }.map { infos: Seq[JobInfo] =>
        logger.info("cleaning {} non-final state job(s) {} for context {}",
          infos.size.toString, infos.map(_.jobId).mkString(", "), contextId)
        for (info <- infos) {
          val updatedInfo = info.copy(
            state = JobStatus.Error,
            endTime = Some(endTime),
            error = Some(ErrorData(ContextTerminatedException(contextId))))
          self ! SaveJobInfo(updatedInfo)
        }
      }

    case GetJobInfosByContextId(contextId, jobStatuses) =>
      Utils.timedFuture(jobQuery){
        metaDataDAO.getJobsByContextId(contextId, jobStatuses)
      }.map(JobInfos).pipeTo(sender)

    case UpdateContextById(contextId: String, attributes: ContextModifiableAttributes) =>
      val recipient = sender()
      Utils.timedFuture(contextRead){
        metaDataDAO.getContext(contextId)
      }.map(ContextResponse).onComplete {
        case Success(ContextResponse(Some(contextInfo))) =>
          saveContextAndRespond(recipient, copyAttributes(contextInfo, attributes))
        case Success(ContextResponse(None)) =>
          logger.warn(s"Context with id $contextId doesn't exist")
          recipient ! SaveFailed(NoMatchingDAOObjectException())
        case Failure(t) =>
          logger.error(s"Failed to get context $contextId by Id", t)
          recipient ! SaveFailed(t)
      }

    case GetBinaryInfosForCp(cp) =>
      val recipient = sender()
      val currentTime = ZonedDateTime.now()
      Try {
        cp.flatMap(name => {
          val uri = new URI(name)
          uri.getScheme match {
            // protocol like "local" is supported in Spark for Jar loading, but not supported in Java.
            case "local" =>
              Some(BinaryInfo("file://" + uri.getPath, BinaryType.URI, currentTime, None))
            case null =>
              val binInfo = Await.result(
                Utils.usingTimer(binRead){ () => metaDataDAO.getBinary(name)}, defaultAwaitTime)
              if (binInfo.isEmpty) {
                throw NoSuchBinaryException(name)
              }
              binInfo
            case _ => Some(BinaryInfo(name, BinaryType.URI, currentTime, None))
          }
        })
      } match {
        case Success(binInfos) => recipient ! BinaryInfosForCp(binInfos)
        case Failure(NoSuchBinaryException(name)) => recipient ! BinaryNotFound(name)
        case Failure(exception) =>
          logger.error(exception.getMessage)
          recipient ! GetBinaryInfosForCpFailed(exception)
    }

    case SaveJobResult(jobId, result) =>
      val recipient = sender()
      Utils.usingTimer(resultWrite) { () =>
        val byteArray = serialize(result)
        binaryDAO.saveJobResult(jobId, byteArray)
      }.onComplete {
        case Success(true) => recipient ! SavedSuccessfully
        case Success(false) =>
          logger.error(s"Failed to save job result for job ${jobId}. DAO returned false.")
          recipient ! SaveFailed(new Throwable(s"Failed to save job result for job (${jobId})."))
        case Failure(t) =>
          logger.error(s"Failed to save job result for job ${jobId} in DAO.", t)
          recipient ! SaveFailed(t)
      }

    case GetJobResult(jobId) =>
      val recipient = sender()
      Utils.usingTimer(resultRead) { () =>
        binaryDAO.getJobResult(jobId)
      }.onComplete {
        case Success(Some(byteArray)) =>
          recipient ! JobResult(deserialize(byteArray))
        case Success(None) =>
          recipient ! JobResult(None)
        case Failure(_) =>
          logger.error(s"Failed to get a job result for job id $jobId")
          recipient ! JobResult(None)
      }

    case CleanupJobs(ageInHours) =>
      val cutoffDate = ZonedDateTime.now().minusHours(ageInHours)
      metaDataDAO.getFinalJobsOlderThan(cutoffDate).onComplete{
        case Success(jobs) =>
          metaDataDAO.deleteJobs(jobs.map(_.jobId))
          binaryDAO.deleteJobResults(jobs.map(_.jobId))
      }

    case CleanupContexts(ageInHours) =>
      val cutoffDate = ZonedDateTime.now().minusHours(ageInHours)
      metaDataDAO.deleteFinalContextsOlderThan(cutoffDate)

    case anotherEvent => logger.info(s"Ignoring unknown event type: $anotherEvent")

  }

  private def saveContextAndRespond(recipient: ActorRef, contextInfo: ContextInfo) = {
    Utils.usingTimer(contextWrite) {
      () => metaDataDAO.saveContext(contextInfo)
    }.onComplete {
        case Success(true) => recipient ! SavedSuccessfully
        case Success(false) =>
          logger.error(s"Failed to save context (${contextInfo.id}). DAO returned false.")
          recipient ! SaveFailed(SaveContextException(contextInfo.id))
        case Failure(t) =>
          logger.error(s"Failed to save context (${contextInfo.id}) in DAO", t)
          recipient ! SaveFailed(t)
      }
    }

  private def copyAttributes(contextInfo: ContextInfo,
                 attributes: ContextModifiableAttributes): ContextInfo = {
    contextInfo.copy(
      actorAddress = getOrDefault(attributes.actorAddress, contextInfo.actorAddress),
      endTime = getOrDefault(attributes.endTime, contextInfo.endTime),
      state = getOrDefault(attributes.state, contextInfo.state),
      error = getOrDefault(attributes.error, contextInfo.error))
  }

  private def getOrDefault[T](value: T, default: T): T = {
    value match {
      case Some(_) => value
      case None => default
      case s: String if s.isEmpty => default
      case _: String => value
    }
  }
}
