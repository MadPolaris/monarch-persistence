package net.imadz.monarch.persistence

/** Host-provided encoding for the business state carried by clean/resolved checkpoints.
  *
  * Protobuf encodes the outer durable protocol; this codec supplies its state bytes.
  * Encoding compatibility and durable artifact availability are host responsibilities.
  * Keep state compact, and ensure old checkpoint bytes still decode after deploys.
  * This codec does not serialize the generic remote StartRun command or its ActorRef.
  */
trait StateCodec[S] {
  def encode(state: S): Array[Byte]
  def decode(bytes: Array[Byte]): S
}

object StateCodec {

  def apply[S](enc: S => Array[Byte], dec: Array[Byte] => S): StateCodec[S] =
    new StateCodec[S] {
      def encode(state: S): Array[Byte] = enc(state)
      def decode(bytes: Array[Byte]): S = dec(bytes)
    }

  /** UTF-8 string codec — handy for tests and stringly-typed states. */
  implicit val stringCodec: StateCodec[String] =
    StateCodec(_.getBytes("UTF-8"), new String(_, "UTF-8"))
}
