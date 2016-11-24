package zmq

import PB.MSGCARRIER.Msgcarrier.MsgCarrier
import kafka.Producer



class ZmqSub extends Runnable{

  private val PUBLISHER_ADDRESS = "tcp://192.168.4.21:8147"
  private val TRANSACTION_TYPE = "94."

  val context = org.zeromq.ZMQ.context(1)
  val subscriber  = context.socket(org.zeromq.ZMQ.SUB)
  subscriber.connect(PUBLISHER_ADDRESS)
  subscriber.subscribe(TRANSACTION_TYPE.getBytes)//94 : transaction


  override def run(): Unit = {

    while(true){
      try{
        subscriber.recv()
        val data = subscriber.recv()

        val mc = MsgCarrier.parseFrom(data)
        if(mc.getType == MsgCarrier.MsgType.TRANSACTIONS){
          val transactions = PB.Quote.Quote.Transactions.parseFrom(mc.getMessage)
          val itemCount = transactions.getItemsCount
          for(i <- 0 until itemCount){
            val transaction = transactions.getItems(i)

            val k = transaction.getCode.getBytes
            val v = transaction.toByteArray

            Producer.produce(k,v)
          }
        }
      }catch{
        case e : Exception => println("zmq error :" + e.getMessage)
      }
    }
  }
}
