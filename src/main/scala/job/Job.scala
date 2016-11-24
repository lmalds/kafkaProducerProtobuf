package job

import zmq.ZmqSub

object Job {

  def main(args : Array[String]) : Unit = {
    new Thread(new ZmqSub()).start()
  }

}
