package io.parapet
import io.parapet.core.Event.Start
import scalaz.zio.Task
import io.parapet.core.Process

object MyApp extends ZioApp {

  import dsl._

  val process = Process[Task](_ => {
    case Start => eval(println("hi zio"))
  })
  override def processes: Task[Seq[core.Process[Task]]] = Task.apply(Seq(process))
}
