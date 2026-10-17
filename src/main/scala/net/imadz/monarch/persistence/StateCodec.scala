package net.imadz.monarch.persistence

/** Host-provided serialization for the run-state snapshot journaled in `StageDone`.
  *
  * The journal stores `Array[Byte]` (Jackson-friendly); how your `S` becomes bytes
  * is host policy — circe / play-json / upickle, protobuf, or plain Java serialization.
  * Keep the encoding stable across deploys: old snapshots must still decode after
  * a restart, otherwise recovery cannot rebuild the resume state.
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
