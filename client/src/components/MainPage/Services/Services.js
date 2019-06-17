import React from "react";
import Code from "react-code-prettify";

import s from "./Services.module.scss";

const Services = () => {
  const markDown = `                              
    import cats.Parallel
    import cats.effect.IO
    import cats.effect.IO._
    import cats.instances.list._
    import io.parapet.core.Flow.Dsl._
    import io.parapet.core.{Component, Event}
    import io.parapet.instances.component._
    import io.parapet.instances.flow._
    import io.parapet.syntax.event._

    import ContextImplicits._

    object DemoApp {
      type Ref = Component[IO]
      // define custom event types
      case class TextMsg(sender: Ref, msg: String) extends Event
      object Start extends Event

      val parapet: Ref = Component[IO] {
        handle {
          case TextMsg(reply, msg) => log(msg) ++ (TextMsg(parapet, "Work hard or Die") ~> reply)
        }
      }

      val mario: Ref = Component[IO] {
        handle {
          case Start => TextMsg(mario, "Hi Parapet, that's me, Mario") ~> parapet
          case TextMsg(_, msg) => log(msg) ++ log("I will !!! But where is Luigi ???")
        }
      }

      val launcher = Start ~> mario // this is not a component

      val program = Parallel.parSequence(List(parapet.run(), mario.run(), launcher.runPar()))

      def main(args: Array[String]): Unit = {
        program.unsafeRunSync()
      }
    }`;
  return (
    <div className="mainContainer">
      <div className={s.sectionTitle}>
        <h1>Parapet</h1>
        <span>Built for distributed engineers and enthusiasts</span>
      </div>
      <div className={s.sectionTitle}>
        <h2>Demo App (Beta)</h2>
      </div>
      <Code codeString={markDown} language="scala" />
    </div>
  );
};

export default Services;
