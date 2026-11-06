package net.imadz.monarch.persistence

import akka.util.ByteString

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/** Bounds library-owned payloads. Host I/O and business allocations need their own limits. */
final case class ProtocolLimits(
    maxRequestBytes: Int = 64 * 1024,
    maxStateBytes: Int = 1024 * 1024,
    maxResultBytes: Int = 1024 * 1024,
    maxEvidenceBytes: Int = 64 * 1024,
    maxEnvelopeBytes: Int = 4 * 1024 * 1024,
    maxStringBytes: Int = 4096,
    maxPlanSteps: Int = 1024,
    maxMetadataEntries: Int = 64
) {
  require(Vector(maxRequestBytes, maxStateBytes, maxResultBytes, maxEvidenceBytes,
    maxEnvelopeBytes, maxStringBytes, maxPlanSteps, maxMetadataEntries).forall(_ > 0),
    "protocol limits must be positive")
  require(maxEnvelopeBytes.toLong >= maxRequestBytes.toLong + maxResultBytes + maxEvidenceBytes,
    "envelope must accommodate a request, result and evidence")
  require(maxEnvelopeBytes >= maxStateBytes, "envelope must accommodate state bytes")
  require(maxRequestBytes <= 64 * 1024 && maxStateBytes <= 1024 * 1024 && maxResultBytes <= 1024 * 1024 &&
    maxEvidenceBytes <= 64 * 1024 && maxEnvelopeBytes <= 4 * 1024 * 1024 && maxStringBytes <= 4096 &&
    maxPlanSteps <= 1024 && maxMetadataEntries <= 64, "R1 limits may narrow, but not exceed, the wire protocol ceilings")

  def text(name: String, value: String, allowEmpty: Boolean = false): Option[String] =
    if (value == null || (!allowEmpty && value.isEmpty)) Some(s"$name must not be empty")
    else if (value.getBytes(StandardCharsets.UTF_8).length > maxStringBytes) Some(s"$name exceeds $maxStringBytes bytes")
    else None

  /** Diagnostics must also fit a reply envelope, including exceptions supplied by a host codec. */
  def diagnostic(value: String): String = {
    val input = Option(value).getOrElse("unavailable")
    val result = new StringBuilder
    var index = 0
    var size = 0
    while (index < input.length && size < maxStringBytes) {
      val point = input.codePointAt(index)
      val encoded = new String(Character.toChars(point))
      val length = encoded.getBytes(StandardCharsets.UTF_8).length
      if (size + length > maxStringBytes) return result.result()
      result.append(encoded)
      size += length
      index += Character.charCount(point)
    }
    result.result()
  }

  def bytes(name: String, value: ByteString, maximum: Int): Option[String] =
    if (value == null) Some(s"$name must not be null")
    else if (value.length > maximum) Some(s"$name exceeds $maximum bytes")
    else None

  def plan(cursors: Vector[String]): Option[String] =
    if (cursors.size > maxPlanSteps) Some(s"plan exceeds $maxPlanSteps steps")
    else cursors.iterator.flatMap(text("cursor", _)).take(1).toVector.headOption.orElse {
      if (cursors.iterator.map(_.getBytes(StandardCharsets.UTF_8).length.toLong).sum > maxEnvelopeBytes / 4)
        Some("plan text exceeds its envelope budget") else None
    }

  def metadata(values: Map[String, String]): Option[String] =
    if (values.size > maxMetadataEntries) Some(s"metadata exceeds $maxMetadataEntries entries")
    else values.iterator.flatMap { case (key, value) =>
      text("metadata key", key).orElse(text("metadata value", value, allowEmpty = true))
    }.take(1).toVector.headOption
}

private[persistence] object ProtocolDigest {
  def sha256(bytes: ByteString): String =
    MessageDigest.getInstance("SHA-256").digest(bytes.toArray).iterator.map(b => f"${b & 0xff}%02x").mkString

  /** Length framing prevents identities such as (ab,c) and (a,bc) from aliasing. */
  def fields(values: Seq[ByteString]): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    values.foreach { value =>
      digest.update(java.nio.ByteBuffer.allocate(4).putInt(value.length).array())
      digest.update(value.toArray)
    }
    digest.digest().iterator.map(b => f"${b & 0xff}%02x").mkString
  }
}
