package spark.jobserver

import scala.collection.mutable
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import akka.actor._
import akka.cluster.Member
import akka.cluster.ClusterEvent.MemberUp
import akka.pattern.gracefulStop
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver.common.akka.InstrumentedActor
import spark.jobserver.JobManagerActor._
import spark.jobserver.util.SparkJobUtils

/** Messages common to all ContextSupervisors */
object ContextSupervisor {
  // Messages/actions
  case object AddContextsFromConfig // Start up initial contexts
  case object ListContexts
  case class AddContext(name: String, contextConfig: Config)
  case class StartAdHocContext(classPath: String, contextConfig: Config)
  case class GetContext(name: String) // returns JobManager, JobResultActor
  case class GetResultActor(name: String)  // returns JobResultActor
  case class StopContext(name: String, retry: Boolean = true)
  case class GetSparkWebUI(name: String)

  // Errors/Responses
  case object ContextInitialized
  case class ContextInitError(t: Throwable)
  case class ContextStopError(t: Throwable)
  case object ContextAlreadyExists
  case object NoSuchContext
  case object ContextStopped
  case class WebUIForContext(name: String, url: Option[String])
}

/**
 * Abstract class that defines all the messages to be implemented
 * in other supervisors
 */
abstract class BaseSupervisorActor(dao: ActorRef, dataManagerActor: ActorRef)
  extends InstrumentedActor {
  import ContextSupervisor._
  import scala.collection.JavaConverters._
  import scala.concurrent.duration._

  val config: Config = context.system.settings.config
  val defaultContextConfig: Config = config.getConfig("spark.context-settings")
  val contextDeletionTimeout: Int = SparkJobUtils.getContextDeletionTimeout(config)

  import context.dispatcher   // to get ExecutionContext for futures
  import akka.pattern.ask

  // This is for capturing results for ad-hoc jobs. Otherwise when ad-hoc job dies, resultActor also dies,
  // and there is no way to retrieve results.
  val globalResultActor: ActorRef = context.actorOf(Props[JobResultActor], "global-result-actor")

  type SJSContext = (String, Option[ActorRef], Option[ActorRef])
  // actor name -> (String contextActorName, Option[JobManagerActor] ref, Option[ResultActor] ref)
  private val contexts = mutable.HashMap.empty[String, SJSContext]

  def wrappedReceive: Receive = {
    case MemberUp(member) =>
      onMemberUp(member)

    case ActorIdentity(memberActors, actorRefOpt) =>
      onActorIdentity(memberActors, actorRefOpt)

    case AddContextsFromConfig =>
      addContextsFromConfig(config)

    case ListContexts =>
      sender ! listContexts()

    case GetSparkWebUI(name) =>
      getContextOpt(name) match {
        case Some((_, Some(actor), _)) =>
          val future = (actor ? GetSparkWebUIUrl)(getTimeout.seconds)
          val originator = sender
          future.collect {
            case SparkWebUIUrl(webUi) => originator ! WebUIForContext(name, Some(webUi))
            case NoSparkWebUI => originator ! WebUIForContext(name, None)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              originator ! NoSuchContext
          }
        case None => sender ! NoSuchContext
      }

    case AddContext(name, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      if (haveContext(name)) {
        originator ! ContextAlreadyExists
      } else {
        registerAndStartContext(name, mergedConfig, isAdHoc = false)(
          (ref) => originator ! ContextInitialized,
          (err) => originator ! ContextInitError(err)
        )
      }

    case StartAdHocContext(classPath, contextConfig) =>
      val originator = sender()
      val mergedConfig = contextConfig.withFallback(defaultContextConfig)
      val userNamePrefix = Try(mergedConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM))
        .map(SparkJobUtils.userNamePrefix).getOrElse("")

      var contextName = ""
      do {
        contextName = userNamePrefix +
          java.util.UUID.randomUUID().toString().substring(0, 8) + "-" + classPath
      } while (haveContext(contextName))
      registerAndStartContext(contextName, mergedConfig, isAdHoc = true)(
        (ref) => originator ! getContext(contextName),
        (err) => originator ! ContextInitError(err)
      )

    case GetResultActor(name) =>
      if (haveContext(name)) {
        sender ! getContext(name)._3.getOrElse(globalResultActor)
      } else {
        sender ! globalResultActor
      }

    case GetContext(name) =>
      if (haveContext(name)) {
        val actorOpt = getContext(name)._2
        if (actorOpt.isDefined) {
          val future = (actorOpt.get ? SparkContextStatus)(getTimeout.seconds)
          val originator = sender
          future.collect {
            case SparkContextAlive => originator ! getContext(name)
            case SparkContextDead =>
              logger.info("SparkContext {} is dead", name)
              self ! StopContext(name)
              originator ! NoSuchContext
          }
        }
      } else {
        sender ! NoSuchContext
      }

    case StopContext(name, retry) =>
      if (haveContext(name)) {
        val contextActorOpt = getContext(name)._2
        if (contextActorOpt.isDefined) {
          try {
            logger.info("Shutting down context {}", name)
            val stoppedCtx = gracefulStop(contextActorOpt.get, contextDeletionTimeout.seconds)
            Await.result(stoppedCtx, (contextDeletionTimeout + 1).seconds)
            removeContext(name)
            onStopContext(contextActorOpt.get)
            sender ! ContextStopped
          }
          catch {
            case err: Exception => sender ! ContextStopError(err)
          }
        } else if (retry) {
          logger.info(
            "Context {} is not yet started, will try and stop after {} seconds",
            name,
            getTimeout
          )
          // send another stopContext, but make the original sender appear as originator
          context.system.scheduler.scheduleOnce(
            getTimeout.seconds, self, StopContext(name, false)
          )(context.dispatcher, sender)
        } else {
          logger.error("Context {} never started and was not removed, unexpected!, stopping!")
          self ! PoisonPill
        }
      } else {
        // if not in retry, its possible something else removed the context
        // be safe and signal that the context was stopped
        if (retry) {
          sender ! NoSuchContext
        } else {
          sender ! ContextStopped
        }
      }

    case Terminated(actorRef) =>
      val actorName: String = actorRef.path.name
      logger.info("Actor terminated: {}", actorName)
      val cxtName = findContextsByActor(actorRef).map(_._1)
      if (cxtName.isEmpty) {
        logger.error("expected to find context, but cannot")
      } else {
        onTerminate(cxtName.get, actorRef)
      }
  }

  protected def haveContext(name: String): Boolean = contexts.contains(name)
  protected def getContext(name: String): SJSContext = contexts(name)
  protected def getContextOpt(name: String): Option[SJSContext] = contexts.get(name)
  protected def removeContext(cxtName: String): Unit = {
    contexts.remove(cxtName)
  }
  protected def removeContextByActorName(actorName: String): Unit = {
    contexts.retain { case (_, (curName, _, _)) => actorName != curName }
  }
  protected def listContexts(): Seq[String] = contexts.keys.toSeq

  protected def addContextStarting(name: String, actorName: String): Unit = {
    contexts(name) = (actorName, None, None)
  }
  protected def addContext(name: String, context: SJSContext): Unit = {
    contexts(name) = context
  }
  protected def findContextsByActor(actorRef: ActorRef): Option[(String, SJSContext)] = {
    contexts.find {
      case (_, (_, Some(saveActor), _)) => saveActor == actorRef
    }
  }

  protected def onTerminate(cxtName: String, ref: ActorRef): Unit
  protected def onMemberUp(member: Member): Unit
  protected def onActorIdentity(memberActors: Any, actorRefOpt: Option[ActorRef]): Unit
  protected def onStopContext(actor: ActorRef): Unit
  protected def getTimeout: Long = SparkJobUtils.getContextCreationTimeout(config)
  protected def getResultActorRef: Option[ActorRef]

  protected def startContext(name: String,
                             actorName: String,
                             contextConfig: Config,
                             isAdHoc: Boolean
                            )(successFunc: ActorRef => Unit,
                              failureFunc: Throwable => Unit): Unit


  protected def buildActorName(): String = "jobManager-" + java.util.UUID.randomUUID().toString.substring(16)

  protected def registerAndStartContext(name: String, contextConfig: Config, isAdHoc: Boolean
  )(successFunc: ActorRef => Unit, failureFunc: Throwable => Unit): Unit = {
    logger.info("Creating a SparkContext named {}", name)
    val actorName = buildActorName()
    addContextStarting(name, actorName)
    startContext(name, actorName, contextConfig, isAdHoc)(successFunc, failureFunc)
  }

  protected def initContext(actorName: String, ref: ActorRef,
                            config: Config, timeoutSecs: Long = 1)
                           (isAdHoc: Boolean,
                            successFunc: ActorRef => Unit,
                            failureFunc: Throwable => Unit): Unit = {
    val resultActorRef = if (isAdHoc) Some(globalResultActor) else getResultActorRef
    (ref ? JobManagerActor.Initialize(
      config, resultActorRef, dataManagerActor))(Timeout(timeoutSecs.second)).onComplete {
      case Failure(e: Exception) =>
        logger.info("Failed to send initialize message to context " + ref, e)
        removeContextByActorName(actorName)
        ref ! PoisonPill
        failureFunc(e)
      case Success(JobManagerActor.InitError(t)) =>
        logger.info("Failed to initialize context " + ref, t)
        removeContextByActorName(actorName)
        ref ! PoisonPill
        failureFunc(t)
      case Success(JobManagerActor.Initialized(ctxName, resActor)) =>
        logger.info("SparkContext {} joined", ctxName)
        // re-add the context, now with references
        addContext(ctxName, (actorName, Some(ref), Some(resActor)))
        context.watch(ref)
        successFunc(ref)
      case _ =>
        logger.info("Failed for unknown reason.")
        removeContextByActorName(actorName)
        ref ! PoisonPill
        failureFunc(new RuntimeException("Failed for unknown reason."))
    }
  }

  // sleep between context creation
  protected def createContextSleep(): Unit = {
    Thread.sleep(500)
  }


  protected def createMergedActorConfig(
    name: String, actorName: String, contextConfig: Config, isAdHoc: Boolean
  ): Config = {
    ConfigFactory.parseMap(
      Map(
        "is-adhoc" -> isAdHoc.toString,
        "context.name" -> name,
        "context.actorname" -> actorName
      ).asJava
    ).withFallback(contextConfig)
  }

  private def addContextsFromConfig(config: Config): Unit = {
    for (contexts <- Try(config.getObject("spark.contexts"))) {
      contexts.keySet().asScala.foreach { contextName =>
        val contextConfig = config.getConfig("spark.contexts." + contextName)
          .withFallback(defaultContextConfig)
        registerAndStartContext(contextName, contextConfig, false)(
          (ref) => Unit,
          (e) => logger.error("Unable to start context" + contextName, e)
        )
        createContextSleep()
      }
    }
  }

}
