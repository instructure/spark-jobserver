package spark.jobserver

import java.io.IOException
import java.nio.file.{Files, Paths}
import java.nio.charset.Charset
import java.util.concurrent.TimeUnit

import akka.actor._
import akka.cluster.{Cluster, Member}
import akka.cluster.ClusterEvent.{InitialStateAsEvents, MemberEvent}
import com.typesafe.config.{Config, ConfigFactory, ConfigRenderOptions}
import spark.jobserver.util.SparkJobUtils

import scala.collection.mutable
import scala.sys.process._
import org.joda.time.DateTime
import org.slf4j.LoggerFactory
import spark.jobserver.io.JobDAOActor.CleanContextJobInfos

import scala.util.Try

/**
 * The AkkaClusterSupervisorActor launches Spark Contexts as external processes
 * that connect back with the master node via Akka Cluster.
 *
 * Currently, when the Supervisor gets a MemberUp message from another actor,
 * it is assumed to be one starting up, and it will be asked to identify itself,
 * and then the Supervisor will try to initialize it.
 *
 * See the [[LocalContextSupervisorActor]] for normal config options.  Here are ones
 * specific to this class.
 *
 * ==Configuration==
 * {{{
 *   deploy {
 *     manager-start-cmd = "./manager_start.sh"
 *   }
 * }}}
 */
class AkkaClusterSupervisorActor(daoActor: ActorRef, dataManagerActor: ActorRef)
    extends BaseSupervisorActor(daoActor, dataManagerActor) {

  import scala.collection.JavaConverters._

  val managerStartCommand: String = config.getString("deploy.manager-start-cmd")

  //actor name -> (context isadhoc, success callback, failure callback)
  //TODO: try to pass this state to the jobManager at start instead of having to track
  //extra state.  What happens if the WebApi process dies before the forked process
  //starts up?  Then it never gets initialized, and this state disappears.
  protected val contextInitInfos =
    mutable.HashMap.empty[String, (Config, Boolean, ActorRef => Unit, Throwable => Unit)]

  private val cluster = Cluster(context.system)
  private val selfAddress = cluster.selfAddress

  logger.info("AkkaClusterSupervisor initialized on {}", selfAddress)

  override def preStart(): Unit = {
    cluster.join(selfAddress)
    cluster.subscribe(self, initialStateMode = InitialStateAsEvents, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    cluster.unsubscribe(self)
    cluster.leave(selfAddress)
  }

  protected def onMemberUp(member: Member): Unit = {
    if (member.hasRole("manager")) {
      val memberActors = RootActorPath(member.address) / "user" / "*"
      context.actorSelection(memberActors) ! Identify(memberActors)
    }
  }

  protected def onActorIdentity(memberActors: Any, actorRefOpt: Option[ActorRef]): Unit = {
    actorRefOpt.foreach { (actorRef) =>
      val actorName = actorRef.path.name
      if (actorName.startsWith("jobManager")) {
        logger.info("Received identify response, attempting to initialize context at {}", memberActors)
        (for { (config, isAdHoc, successFunc, failureFunc) <- contextInitInfos.remove(actorName) }
         yield {
           initContext(actorName, actorRef, config, getTimeout)(isAdHoc, successFunc, failureFunc)
         }).getOrElse({
          logger.warn("No initialization or callback found for jobManager actor {}", actorRef.path)
          actorRef ! PoisonPill
        })
      }
    }
  }

  protected def getResultActorRef: Option[ActorRef] = {
    Some(context.actorOf(Props(classOf[JobResultActor])))
  }

  protected def onTerminate(name: String, actorRef: ActorRef): Unit = {
    for ((name, _) <- findContextsByActor(actorRef)) {
      removeContext(name)
      daoActor ! CleanContextJobInfos(name, DateTime.now())
    }
    cluster.down(actorRef.path.address)
  }

  protected def onStopContext(actor: ActorRef): Unit = {
    cluster.down(actor.path.address)
  }

  protected def startContext(name: String,
                             actorName: String,
                             contextConfig: Config,
                             isAdHoc: Boolean)
                            (successFunc: ActorRef => Unit,
                             failureFunc: Throwable => Unit): Unit = {

    val contextActorName = actorName
    logger.info("Starting context with actor name {}", contextActorName)

    val master = Try(config.getString("spark.master")).toOption.getOrElse("local[4]")
    val deployMode = Try(config.getString("spark.submit.deployMode")).toOption.getOrElse("client")

    // Create a temporary dir, preferably in the LOG_DIR
    val encodedContextName = java.net.URLEncoder.encode(name, "UTF-8")
    val contextDir = Option(System.getProperty("LOG_DIR")).map { logDir =>
      Files.createTempDirectory(Paths.get(logDir), s"jobserver-$encodedContextName")
    }.getOrElse(Files.createTempDirectory("jobserver"))
    logger.info("Created working directory {} for context {}", contextDir: Any, name)

    // Now create the contextConfig merged with the values we need
    val mergedContextConfig = ConfigFactory.parseMap(
      Map("is-adhoc" -> isAdHoc.toString, "context.name" -> name).asJava
    ).withFallback(contextConfig)

    val conStr = mergedContextConfig.root.render(ConfigRenderOptions.concise())
    var managerArgs = Seq(master, deployMode, selfAddress.toString,
      contextActorName, contextDir.toString, conStr)
    // extract spark.proxy.user from contextConfig, if available and pass it to manager start command
    if (contextConfig.hasPath(SparkJobUtils.SPARK_PROXY_USER_PARAM)) {
      managerArgs = managerArgs :+ contextConfig.getString(SparkJobUtils.SPARK_PROXY_USER_PARAM)
    }

    val contextLogger = LoggerFactory.getLogger("manager_start")
    val process = Process(managerStartCommand, managerArgs)

    val processStart = Try {
      val procStat = process.run(ProcessLogger(out => contextLogger.info(out),
        err => contextLogger.warn(err)))
      val exitVal = procStat.exitValue()
      if (exitVal != 0) {
        throw new IOException("Failed to launch context process, got exit code " + exitVal)
      }
    }

    if (processStart.isSuccess) {
      contextInitInfos(contextActorName) = (mergedContextConfig, isAdHoc, successFunc, failureFunc)
    } else {
      failureFunc(processStart.failed.get)
    }
  }

  // we don't need any sleep between contexts in a cluster
  override protected def createContextSleep(): Unit = {
  }


}
