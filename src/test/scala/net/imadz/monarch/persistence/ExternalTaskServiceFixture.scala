package net.imadz.monarch.persistence

import java.io.{BufferedInputStream, BufferedOutputStream, ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream, EOFException, IOException, InputStream, OutputStream}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.nio.{ByteBuffer, CharBuffer}
import java.nio.channels.{FileChannel, FileLock}
import java.nio.charset.{CharacterCodingException, CodingErrorAction, StandardCharsets}
import java.nio.file.{Files, LinkOption, Path, Paths, StandardOpenOption}
import java.util.{Arrays, Base64}
import java.util.zip.CRC32

import scala.collection.mutable
import scala.util.control.NonFatal

/**
  * An independent, parent-controlled external bank/equipment oracle. Run this object's main in a
  * separate JVM with only the test classes and scala-library on its classpath (JDK 11 compatible).
  * Arguments: <absolute-owned-directory> [port, default 0]. It binds IPv4 127.0.0.1, prints and
  * flushes "EXTERNAL_READY <port>", and serves until the parent terminates the process. There is
  * deliberately no shutdown command, timer-driven completion, actor, or operation-module dependency.
  *
  * Protocol (every item below is a string; decimal counts have no formatting):
  *
  *   SUBMIT [kind, operationKey, requestDigest, ledgerEpoch, effectKey, businessPayload]
  *     -> [RUNNING, taskId, ledgerEpoch] or [SUCCEEDED, taskId, ledgerEpoch, result]
  *   QUERY [operationKey]
  *     -> [RUNNING, taskId, ledgerEpoch, operationKey, requestDigest]
  *     or [SUCCEEDED, taskId, ledgerEpoch, result, operationKey, requestDigest]
  *     or [NOT_FOUND, actualLedgerEpoch]
  *   COMPLETE [operationKey]
  *     -> [SUCCEEDED, taskId, ledgerEpoch, result] or [NOT_FOUND, actualLedgerEpoch]
  *   COUNT [] -> [COUNT, uniqueCompletedEffects, acceptedTasks, submitCalls, queryCalls]
  *   BUSINESS [] -> [BUSINESS, bankCreditsCount, bankPurchasesCount, totalBalance, totalHoldings,
  *                   fabFinalAcceptedCount, fabUnloadedCount, duplicateEffects]
  *   TASKS [] -> [TASKS, runningOperationKey, ...] (sorted; at most 15 fields including status)
  *   ITEMS [] -> [ITEMS, bankItems, fabItems], sorted by decoded item ID within each domain:
  *     bankItems = comma-separated <base64url-itemId>~<balance>~<holding>
  *     fabItems  = comma-separated <base64url-itemId>~<location>~<phase>
  *     An empty domain has an empty string. Oversized diagnostic replies are REJECTED.
  *   EPOCH [] -> [EPOCH, actualLedgerEpoch]
  *   FORGET [] -> [EPOCH, newLedgerEpoch]
  *   CHECK [] -> [CHECK, actualLedgerEpoch, acceptedTasks, runningTasks, succeededTasks,
  *               uniqueCompletedEffects]
  *   DROP_NEXT_SUBMIT_REPLY [] -> [OK]
  *
  * A rejection is [REJECTED, reason]. Reasons are INVALID_FIELDS, UNKNOWN_COMMAND,
  * UNSUPPORTED_KIND, EMPTY_IDENTITY_FIELD, IMMUTABLE_FIELDS_CHANGED, LEDGER_EPOCH_MISMATCH,
  * OPERATION_HISTORY_UNAVAILABLE, EFFECT_KEY_CONFLICT, FRAME_LIMIT_EXCEEDED,
  * OWNERSHIP_MARKER_REQUIRED, INVALID_BUSINESS_PAYLOAD, BUSINESS_KIND_MISMATCH,
  * BUSINESS_ORDER_VIOLATION, or BUSINESS_DUPLICATE_EXECUTION. Malformed wire frames are closed
  * without a response. A rejected COMPLETE leaves the original accepted task RUNNING.
  *
  * kind is exactly "bank" or "fab". Except for the opt-in WF2: prefix, payloads remain opaque
  * strings, including empty strings, and results remain exactly "bank-receipt:<operationKey>" or
  * "fab-measurement:<operationKey>". Operation keys, digests, and effect keys must be nonempty.
  * All six SUBMIT fields are immutable.
  *
  * WF2:<area>:<base64url-UTF8-itemId>~<executionNumber>,... contains 1..100 unique, nonempty IDs
  * and execution numbers 0..32. Canonical URL-safe base64 with or without padding is accepted;
  * results/snapshots always use unpadded base64url. Areas recharge/purchase require kind bank;
  * transport-in/litho/transport-out/measure/rework/unload require kind fab. COMPLETE derives
  * WR2:<area>:<base64url-itemId>~<SUCCESS|RETRY|REWORK>,... in request item order. The oracle, not
  * the host or batch codec, chooses each classification using its own durable per-item history.
  * Recharge of an ID ending in 1 at execution 0 is RETRY without credit; other valid recharges
  * credit exactly 100 once. Purchase requires that credit, debits 100 and adds a holding of 100
  * once. Fab follows stocker -> litho -> processed -> metrology -> measurement -> unload/stocker.
  * First measure of an ID ending in 1 at execution 0 yields REWORK, requiring rework before a
  * successful remeasurement; otherwise measurement succeeds. No port-capacity simulation is done.
  * Reusing a completed item execution in another task is rejected, not counted as a fresh effect.
  * A batch is all-or-nothing: any invalid item prevents its entire completion from being appended.
  * A durable acceptance assigns task-1, task-2, ...; the ordinal never resets, even across FORGET.
  * Only QUERY appends the original stored operation key and digest. Duplicate SUBMIT/COMPLETE
  * returns stored evidence without appending another acceptance/completion.
  *
  * acceptedTasks/runningTasks/succeededTasks describe the current epoch's visible task history.
  * uniqueCompletedEffects retains its original meaning: completed physical task/effect keys across
  * ALL epochs, including WF2 tasks whose result contains RETRY. BUSINESS instead counts per-item
  * domain effects, excluding RETRY from credits, and includes effects of forgotten tasks. These
  * counts are reconstructed from durable records. submitCalls/queryCalls are explicitly
  * process-local diagnostics (including rejected calls), reset on service restart. CHECK describes
  * only this service's ledger, never whether the host recovered correctly or may safely resend.
  *
  * FORGET appends an EPOCH event (ledger-1 initially, then ledger-2, ...), hiding old tasks without
  * deleting files. Completed effects, effect reservations (including incomplete tasks), and retired
  * operation-key tombstones survive. Neither forgotten keys nor reserved effects can be reused.
  * QUERY can therefore report NOT_FOUND after a business effect; it is NOT safe-resend evidence.
  * A valid ownership marker is checked again before FORGET. An existing unmarked, nonempty directory
  * is never claimed. DROP_NEXT_SUBMIT_REPLY is process-local: the next successful SUBMIT, including
  * a durable duplicate, closes without a reply; rejected submissions do not consume the flag.
  *
  * Wire format, big endian: int32 bodyLength, int32 stringCount, then stringCount repetitions of
  * int32 utf8ByteLength + strict standard UTF-8 bytes (NOT Java modified UTF/writeUTF). The command
  * is the first request string and the status is the first response string. At most 16 strings
  * including command/status, at most 65,536 UTF-8 bytes per string, and at most 131,072 bytes for the
  * entire wire frame including its length prefix. Acceptance preflights ledger and future evidence
  * frames so an oversized derived receipt cannot leave an unqueryable accepted task. call uses
  * 2,000 ms connect/read timeouts and one connection per command; a dropped reply throws IOException.
  *
  * Owned files: .external-task-service-fixture (fixed version marker), external-task-service.wal.
  * WAL records, big endian: int32 magic 0x45545331 (ETS1), int32 bodyLength, int32 headerCRC32
  * (over the preceding eight bytes), body in the same string-vector encoding as the wire body,
  * int32 bodyCRC32. CRC32 values use their low 32 bits. Events are:
  *   [EPOCH, epoch]
  *   [ACCEPT, kind, operationKey, requestDigest, epoch, effectKey, businessPayload, taskId]
  *   [COMPLETED, operationKey, taskId, epoch, effectKey, result]
  * Each append is force(true)'d before publication/reply. A single COMPLETED event is both task
  * history and the business effect itself, not a callback counter. For WF2, replay validates the
  * stored structured result against the accepted request and prior domain state, then reconstructs
  * all wallets, holdings, FOUP locations and per-item execution evidence from that one record.
  * Replay only rebuilds state; it neither appends completions nor emits EXTERNAL_EFFECT. Incomplete
  * trailing records alone
  * are truncated and forced on startup; complete frames with bad checksums/schema fail startup.
  * A lifetime exclusive WAL lock prevents two services from using the same owned directory.
  */
object ExternalTaskServiceFixture {
  private val MaxStrings = 16
  private val MaxStringBytes = 64 * 1024
  private val MaxWireBytes = 128 * 1024
  private val MaxBodyBytes = MaxWireBytes - 4
  private val SocketTimeoutMillis = 2000
  private val RecordMagic = 0x45545331
  private val RecordHeaderBytes = 12
  private val MarkerName = ".external-task-service-fixture"
  private val JournalName = "external-task-service.wal"
  private val MarkerBytes = "ExternalTaskServiceFixture v1\n".getBytes(StandardCharsets.UTF_8)
  private val Loopback = InetAddress.getByName("127.0.0.1")

  def main(args: Array[String]): Unit = {
    require(args.length == 1 || args.length == 2,
      "Usage: ExternalTaskServiceFixture <absolute-owned-directory> [port]")
    val directory = Paths.get(args(0))
    require(directory.isAbsolute, "The fixture directory must be absolute")
    val port = if (args.length == 2) args(1).toInt else 0
    require(port >= 0 && port <= 65535, "The port must be between 0 and 65535")

    val ledger = openLedger(directory.normalize())
    val server = new ServerSocket()
    try {
      server.setReuseAddress(false)
      server.bind(new InetSocketAddress(Loopback, port))
      diagnostic(s"EXTERNAL_READY ${server.getLocalPort}")
      while (true) {
        val socket = server.accept()
        try {
          socket.setSoTimeout(SocketTimeoutMillis)
          socket.setTcpNoDelay(true)
          serveConnection(socket, ledger)
        } finally closeQuietly(socket)
      }
    } finally {
      try server.close()
      finally ledger.close()
    }
  }

  def call(port: Int, command: String, fields: Vector[String] = Vector.empty): Vector[String] = {
    require(port > 0 && port <= 65535, "The service port must be between 1 and 65535")
    val request = encodeBody(command +: fields)
    val socket = new Socket()
    try {
      socket.connect(new InetSocketAddress(Loopback, port), SocketTimeoutMillis)
      socket.setSoTimeout(SocketTimeoutMillis)
      socket.setTcpNoDelay(true)
      writeFrame(socket.getOutputStream, request)
      readFrame(socket.getInputStream)
    } finally closeQuietly(socket)
  }

  private def serveConnection(socket: Socket, ledger: Ledger): Unit = {
    val request = try Some(readFrame(socket.getInputStream))
    catch { case _: IOException => None }

    request.foreach { strings =>
      // Deliberately do not catch journal errors here: after a failed append/force the service
      // must fail closed, not continue with possibly divergent memory and durable state.
      val response = ledger.execute(strings)
      response.foreach { fields =>
        val body = encodeBody(fields)
        try writeFrame(socket.getOutputStream, body)
        catch { case _: IOException => () } // Losing a client never undoes durable acceptance/effect.
      }
    }
  }

  private final class FrameException(message: String, cause: Throwable = null)
    extends IOException(message, cause)

  private def encodeBody(fields: Vector[String]): Array[Byte] = {
    if (fields.isEmpty || fields.size > MaxStrings)
      throw new FrameException("A frame must contain between 1 and 16 strings")
    val bytes = new ByteArrayOutputStream()
    val out = new DataOutputStream(bytes)
    out.writeInt(fields.size)
    fields.foreach { field =>
      if (field == null || field.length > MaxStringBytes)
        throw new FrameException("Null or oversized string")
      val encoded = try {
        StandardCharsets.UTF_8.newEncoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .encode(CharBuffer.wrap(field))
      } catch {
        case e: CharacterCodingException => throw new FrameException("Invalid UTF string", e)
      }
      val length = encoded.remaining()
      if (length > MaxStringBytes || bytes.size().toLong + 4L + length > MaxBodyBytes)
        throw new FrameException("Frame/string byte limit exceeded")
      val value = new Array[Byte](length)
      encoded.get(value)
      out.writeInt(length)
      out.write(value)
    }
    out.flush()
    bytes.toByteArray
  }

  private def decodeBody(body: Array[Byte]): Vector[String] = {
    val in = new DataInputStream(new ByteArrayInputStream(body))
    val count = in.readInt()
    if (count < 1 || count > MaxStrings) throw new FrameException("Invalid string count")
    val fields = Vector.newBuilder[String]
    var index = 0
    while (index < count) {
      val length = in.readInt()
      if (length < 0 || length > MaxStringBytes || length > in.available())
        throw new FrameException("Invalid string byte length")
      val bytes = new Array[Byte](length)
      in.readFully(bytes)
      val value = try {
        StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes)).toString
      } catch {
        case e: CharacterCodingException => throw new FrameException("Invalid UTF-8", e)
      }
      fields += value
      index += 1
    }
    if (in.available() != 0) throw new FrameException("Trailing bytes in a frame")
    fields.result()
  }

  private def writeFrame(stream: OutputStream, body: Array[Byte]): Unit = {
    val out = new DataOutputStream(new BufferedOutputStream(stream))
    out.writeInt(body.length)
    out.write(body)
    out.flush()
  }

  private def readFrame(stream: InputStream): Vector[String] = {
    val in = new DataInputStream(new BufferedInputStream(stream))
    val length = in.readInt()
    if (length < 4 || length > MaxBodyBytes) throw new FrameException("Invalid frame byte length")
    val body = new Array[Byte](length)
    in.readFully(body)
    decodeBody(body)
  }

  private final case class AcceptedTask(
      kind: String,
      operationKey: String,
      requestDigest: String,
      ledgerEpoch: String,
      effectKey: String,
      businessPayload: String,
      taskId: String) {
    def submitFields: Vector[String] =
      Vector(kind, operationKey, requestDigest, ledgerEpoch, effectKey, businessPayload)
    def acceptance: Vector[String] = Vector("ACCEPT") ++ submitFields :+ taskId
    def completion(result: String): Vector[String] =
      Vector("COMPLETED", operationKey, taskId, ledgerEpoch, effectKey, result)
    def completedQuery(result: String): Vector[String] =
      Vector("SUCCEEDED", taskId, ledgerEpoch, result, operationKey, requestDigest)
  }

  private val BankAreas = Set("recharge", "purchase")
  private val FabAreas = Set("transport-in", "litho", "transport-out", "measure", "rework", "unload")
  private val ItemEncoder = Base64.getUrlEncoder.withoutPadding()

  private final case class WorkflowItem(id: String, encodedId: String, executionNumber: Int)
  private final case class WorkflowPayload(area: String, items: Vector[WorkflowItem]) {
    def result(classifications: Vector[String]): String =
      s"WR2:$area:" + items.zip(classifications).map { case (item, classification) =>
        s"${item.encodedId}~$classification"
      }.mkString(",")

    // SUCCESS is longer than RETRY or REWORK, so this bounds every possible future reply.
    def largestResult: String = result(Vector.fill(items.size)("SUCCESS"))
  }

  private def parseWorkflow(kind: String, payload: String): Either[String, Option[WorkflowPayload]] = {
    if (!payload.startsWith("WF2:")) return Right(None)
    val parts = payload.split(":", -1)
    if (parts.length != 3 || (!BankAreas.contains(parts(1)) && !FabAreas.contains(parts(1))))
      return Left("INVALID_BUSINESS_PAYLOAD")
    val area = parts(1)
    if ((BankAreas.contains(area) && kind != "bank") || (FabAreas.contains(area) && kind != "fab"))
      return Left("BUSINESS_KIND_MISMATCH")
    val entries = parts(2).split(",", -1).toVector
    if (entries.size > 100) return Left("INVALID_BUSINESS_PAYLOAD")
    try {
      val items = entries.map { entry =>
        val fields = entry.split("~", -1)
        require(fields.length == 2 && fields(0).nonEmpty, "invalid item shape")
        val bytes = Base64.getUrlDecoder.decode(fields(0))
        val encodedId = ItemEncoder.encodeToString(bytes)
        require(fields(0) == encodedId || fields(0) == Base64.getUrlEncoder.encodeToString(bytes),
          "noncanonical base64url item ID")
        val id = StandardCharsets.UTF_8.newDecoder()
          .onMalformedInput(CodingErrorAction.REPORT)
          .onUnmappableCharacter(CodingErrorAction.REPORT)
          .decode(ByteBuffer.wrap(bytes)).toString
        require(id.nonEmpty, "empty item ID")
        require(fields(1).nonEmpty && fields(1).forall(c => c >= '0' && c <= '9'),
          "invalid execution number")
        val execution = fields(1).toInt
        require(execution >= 0 && execution <= 32, "execution number out of range")
        WorkflowItem(id, encodedId, execution)
      }
      if (items.map(_.id).distinct.size != items.size) Left("INVALID_BUSINESS_PAYLOAD")
      else Right(Some(WorkflowPayload(area, items)))
    } catch {
      case _: IllegalArgumentException => Left("INVALID_BUSINESS_PAYLOAD")
      case _: CharacterCodingException => Left("INVALID_BUSINESS_PAYLOAD")
    }
  }

  private final case class ItemEvidence(
      area: String, executionNumber: Int, classification: String,
      operationKey: String, taskId: String, ledgerEpoch: String, effectKey: String)

  private final case class BankAccount(
      balance: Long = 0L, holding: Long = 0L, history: Vector[ItemEvidence] = Vector.empty) {
    def credited: Boolean = history.exists(e => e.area == "recharge" && e.classification == "SUCCESS")
    def purchased: Boolean = history.exists(e => e.area == "purchase" && e.classification == "SUCCESS")
  }

  private final case class FabItem(
      location: String = "stocker", phase: String = "stocker", history: Vector[ItemEvidence] = Vector.empty) {
    def accepted: Boolean = history.exists(e => e.area == "measure" && e.classification == "SUCCESS")
    def unloaded: Boolean = history.exists(e => e.area == "unload" && e.classification == "SUCCESS")
  }

  private final case class BusinessState(
      bank: Map[String, BankAccount] = Map.empty, fab: Map[String, FabItem] = Map.empty) {
    def summary: Vector[String] = {
      // Count actual repeated domain effects, not repeated COMPLETE calls or physical batch keys.
      // A REWORK measurement and its later SUCCESS are distinct, legitimate measurement effects.
      val histories = bank.valuesIterator.map(_.history) ++ fab.valuesIterator.map(_.history)
      val duplicates = histories.map { history =>
        history.filterNot(_.classification == "RETRY")
          .groupBy(e => (e.area, e.classification)).valuesIterator
          .map(events => math.max(0, events.size - 1).toLong).sum
      }.sum
      Vector("BUSINESS", bank.valuesIterator.count(_.credited).toString,
        bank.valuesIterator.count(_.purchased).toString, bank.valuesIterator.map(_.balance).sum.toString,
        bank.valuesIterator.map(_.holding).sum.toString, fab.valuesIterator.count(_.accepted).toString,
        fab.valuesIterator.count(_.unloaded).toString, duplicates.toString)
    }

    def items: Vector[String] = {
      def encoded(id: String): String = ItemEncoder.encodeToString(id.getBytes(StandardCharsets.UTF_8))
      val bankItems = bank.toVector.sortBy(_._1).map { case (id, account) =>
        s"${encoded(id)}~${account.balance}~${account.holding}"
      }.mkString(",")
      val fabItems = fab.toVector.sortBy(_._1).map { case (id, item) =>
        s"${encoded(id)}~${item.location}~${item.phase}"
      }.mkString(",")
      Vector("ITEMS", bankItems, fabItems)
    }
  }

  private final case class PredictedCompletion(result: String, state: BusinessState)

  private def opaqueResult(task: AcceptedTask): String =
    if (task.kind == "bank") s"bank-receipt:${task.operationKey}" else s"fab-measurement:${task.operationKey}"

  // Pure interpretation: no product codec/state, I/O, effect-key-based item identity, or ledger mutation.
  private def predictResult(task: AcceptedTask, state: BusinessState): Either[String, PredictedCompletion] =
    parseWorkflow(task.kind, task.businessPayload).flatMap {
      case None => Right(PredictedCompletion(opaqueResult(task), state))
      case Some(workflow) =>
        workflow.items.foldLeft[Either[String, (Vector[String], BusinessState)]](
          Right((Vector.empty[String], state))) { (accumulated, item) =>
          accumulated.flatMap { case (classifications, prior) =>
            interpretItem(task, workflow.area, item, prior).map { case (classification, next) =>
              (classifications :+ classification, next)
            }
          }
        }.map { case (classifications, next) =>
          PredictedCompletion(workflow.result(classifications), next)
        }
    }

  private def interpretItem(task: AcceptedTask, area: String, item: WorkflowItem,
      state: BusinessState): Either[String, (String, BusinessState)] = {
    def evidence(classification: String): ItemEvidence =
      ItemEvidence(area, item.executionNumber, classification, task.operationKey, task.taskId,
        task.ledgerEpoch, task.effectKey)
    def alreadyExecuted(history: Vector[ItemEvidence]): Boolean =
      history.exists(e => e.area == area && e.executionNumber == item.executionNumber)

    if (BankAreas.contains(area)) {
      val account = state.bank.getOrElse(item.id, BankAccount())
      if (alreadyExecuted(account.history)) return Left("BUSINESS_DUPLICATE_EXECUTION")
      val transition: Either[String, (String, BankAccount)] = area match {
        case "recharge" if account.credited => Left("BUSINESS_DUPLICATE_EXECUTION")
        case "recharge" if item.id.endsWith("1") && item.executionNumber == 0 => Right(("RETRY", account))
        case "recharge" => Right(("SUCCESS", account.copy(balance = 100L)))
        case "purchase" if account.purchased => Left("BUSINESS_DUPLICATE_EXECUTION")
        case "purchase" if account.credited && account.balance == 100L && account.holding == 0L =>
          Right(("SUCCESS", account.copy(balance = 0L, holding = 100L)))
        case _ => Left("BUSINESS_ORDER_VIOLATION")
      }
      transition.map { case (classification, next) =>
        val recorded = next.copy(history = account.history :+ evidence(classification))
        (classification, state.copy(bank = state.bank.updated(item.id, recorded)))
      }
    } else {
      val foup = state.fab.getOrElse(item.id, FabItem())
      if (alreadyExecuted(foup.history) || (area != "measure" && foup.history.exists(_.area == area)))
        return Left("BUSINESS_DUPLICATE_EXECUTION")
      val transition: Either[String, (String, FabItem)] = area match {
        case "transport-in" if foup.location == "stocker" && foup.phase == "stocker" =>
          Right(("SUCCESS", foup.copy(location = "litho", phase = "imported")))
        case "litho" if foup.location == "litho" && foup.phase == "imported" =>
          Right(("SUCCESS", foup.copy(phase = "processed")))
        case "transport-out" if foup.location == "litho" && foup.phase == "processed" =>
          Right(("SUCCESS", foup.copy(location = "metrology", phase = "awaiting-measurement")))
        case "measure" if foup.location == "metrology" &&
            (foup.phase == "awaiting-measurement" || foup.phase == "reworked") =>
          if (foup.phase == "awaiting-measurement" && item.id.endsWith("1") && item.executionNumber == 0)
            Right(("REWORK", foup.copy(phase = "measurement-rework")))
          else Right(("SUCCESS", foup.copy(phase = "accepted")))
        case "rework" if foup.location == "metrology" && foup.phase == "measurement-rework" =>
          Right(("SUCCESS", foup.copy(phase = "reworked")))
        case "unload" if foup.location == "metrology" && foup.phase == "accepted" =>
          Right(("SUCCESS", foup.copy(location = "stocker", phase = "unloaded")))
        case _ => Left("BUSINESS_ORDER_VIOLATION")
      }
      transition.map { case (classification, next) =>
        val recorded = next.copy(history = foup.history :+ evidence(classification))
        (classification, state.copy(fab = state.fab.updated(item.id, recorded)))
      }
    }
  }

  private final case class CompletedEffect(
      operationKey: String, taskId: String, ledgerEpoch: String, result: String)

  private final class Ledger(directory: Path, journal: FileChannel, lock: FileLock)
    extends AutoCloseable {
    private var epochNumber = 0L
    private var taskNumber = 0L
    private val tasks = mutable.HashMap.empty[String, AcceptedTask]
    private val retiredKeys = mutable.HashSet.empty[String]
    private val reservedEffects = mutable.HashSet.empty[String]
    private val completedEffects = mutable.HashMap.empty[String, CompletedEffect]
    private var businessState = BusinessState()
    private var submitCalls = 0L
    private var queryCalls = 0L
    private var dropNextSubmitReply = false

    private def epoch: String = s"ledger-$epochNumber"
    private def rejected(reason: String): Vector[String] = Vector("REJECTED", reason)
    private def notFound: Vector[String] = Vector("NOT_FOUND", epoch)

    def execute(request: Vector[String]): Option[Vector[String]] = synchronized {
      val command = request.head
      val fields = request.tail
      val response = command match {
        case "SUBMIT" =>
          submitCalls += 1
          if (fields.size != 6) rejected("INVALID_FIELDS")
          else {
            diagnostic(s"EXTERNAL_SUBMIT ${fields(1)}")
            submit(fields)
          }
        case "QUERY" =>
          queryCalls += 1
          if (fields.size != 1) rejected("INVALID_FIELDS")
          else {
            diagnostic(s"EXTERNAL_QUERY ${fields.head}")
            tasks.get(fields.head).map(task => status(task, evidence = true)).getOrElse(notFound)
          }
        case "COMPLETE" =>
          if (fields.size != 1) rejected("INVALID_FIELDS")
          else complete(fields.head)
        case "COUNT" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS")
          else Vector("COUNT", completedEffects.size.toString, tasks.size.toString,
            submitCalls.toString, queryCalls.toString)
        case "BUSINESS" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS") else businessState.summary
        case "ITEMS" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS") else boundedReply(businessState.items)
        case "TASKS" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS")
          else {
            val running = tasks.valuesIterator.filterNot(task => completedEffects.contains(task.effectKey))
              .map(_.operationKey).toVector.sorted
            if (running.size + 1 > 15) rejected("FRAME_LIMIT_EXCEEDED")
            else boundedReply("TASKS" +: running)
          }
        case "EPOCH" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS") else Vector("EPOCH", epoch)
        case "FORGET" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS")
          else if (!hasOwnershipMarker(directory)) rejected("OWNERSHIP_MARKER_REQUIRED")
          else {
            append(Vector("EPOCH", s"ledger-${Math.addExact(epochNumber, 1L)}"))
            Vector("EPOCH", epoch)
          }
        case "CHECK" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS")
          else {
            val succeeded = tasks.valuesIterator.count(task => completedEffects.contains(task.effectKey))
            Vector("CHECK", epoch, tasks.size.toString, (tasks.size - succeeded).toString,
              succeeded.toString, completedEffects.size.toString)
          }
        case "DROP_NEXT_SUBMIT_REPLY" =>
          if (fields.nonEmpty) rejected("INVALID_FIELDS")
          else {
            dropNextSubmitReply = true
            Vector("OK")
          }
        case _ => rejected("UNKNOWN_COMMAND")
      }
      if (command == "SUBMIT" && dropNextSubmitReply &&
          (response.head == "RUNNING" || response.head == "SUCCEEDED")) {
        dropNextSubmitReply = false
        None
      } else Some(response)
    }

    private def submit(fields: Vector[String]): Vector[String] = {
      tasks.get(fields(1)) match {
        case Some(task) =>
          if (task.submitFields != fields) rejected("IMMUTABLE_FIELDS_CHANGED")
          else status(task, evidence = false)
        case None =>
          if (fields(0) != "bank" && fields(0) != "fab") return rejected("UNSUPPORTED_KIND")
          if (fields(1).isEmpty || fields(2).isEmpty || fields(4).isEmpty)
            return rejected("EMPTY_IDENTITY_FIELD")
          if (fields(3) != epoch) return rejected("LEDGER_EPOCH_MISMATCH")
          if (retiredKeys.contains(fields(1))) return rejected("OPERATION_HISTORY_UNAVAILABLE")
          if (reservedEffects.contains(fields(4))) return rejected("EFFECT_KEY_CONFLICT")

          val task = AcceptedTask(fields(0), fields(1), fields(2), fields(3), fields(4), fields(5),
            s"task-${Math.addExact(taskNumber, 1L)}")
          val preflight = try ensureTaskFits(task)
          catch { case _: FrameException => return rejected("FRAME_LIMIT_EXCEEDED") }
          preflight match {
            case Left(reason) => return rejected(reason)
            case Right(_) => ()
          }
          append(task.acceptance)
          status(task, evidence = false)
      }
    }

    private def complete(operationKey: String): Vector[String] = {
      tasks.get(operationKey) match {
        case None => notFound
        case Some(task) =>
          if (!completedEffects.contains(task.effectKey)) {
            val predicted = predictResult(task, businessState) match {
              case Left(reason) => return rejected(reason)
              case Right(value) => value
            }
            append(task.completion(predicted.result))
            diagnostic(s"EXTERNAL_EFFECT $operationKey")
          }
          status(task, evidence = false)
      }
    }

    private def status(task: AcceptedTask, evidence: Boolean): Vector[String] = {
      val fields = completedEffects.get(task.effectKey) match {
        case Some(effect) => Vector("SUCCEEDED", effect.taskId, effect.ledgerEpoch, effect.result)
        case None => Vector("RUNNING", task.taskId, task.ledgerEpoch)
      }
      if (evidence) fields ++ Vector(task.operationKey, task.requestDigest) else fields
    }

    private def boundedReply(fields: Vector[String]): Vector[String] = {
      try {
        encodeBody(fields)
        fields
      } catch { case _: FrameException => rejected("FRAME_LIMIT_EXCEEDED") }
    }

    private def ensureTaskFits(task: AcceptedTask): Either[String, Unit] =
      parseWorkflow(task.kind, task.businessPayload).map { workflow =>
        val largestResult = workflow.map(_.largestResult).getOrElse(opaqueResult(task))
        encodeBody(task.acceptance)
        encodeBody(task.completion(largestResult))
        encodeBody(task.completedQuery(largestResult))
        ()
      }

    private def validateResult(task: AcceptedTask, result: String, state: BusinessState): BusinessState =
      predictResult(task, state) match {
        case Left(reason) => throw new IOException(s"Invalid external fixture WAL business transition: $reason")
        case Right(predicted) =>
          requireRecord(predicted.result == result, "completion result mismatch")
          predicted.state
      }

    // The only state transition routine. It performs no I/O, logging, or external callbacks.
    private def applyRecord(record: Vector[String]): Unit = record match {
      case Vector("EPOCH", nextEpoch) =>
        val next = Math.addExact(epochNumber, 1L)
        requireRecord(nextEpoch == s"ledger-$next", "non-sequential epoch")
        retiredKeys ++= tasks.keysIterator
        tasks.clear()
        epochNumber = next
      case Vector("ACCEPT", kind, key, digest, storedEpoch, effectKey, payload, taskId) =>
        requireRecord(epochNumber > 0 && storedEpoch == epoch, "acceptance epoch mismatch")
        requireRecord(kind == "bank" || kind == "fab", "unsupported task kind")
        requireRecord(key.nonEmpty && digest.nonEmpty && effectKey.nonEmpty, "empty task identity")
        val next = Math.addExact(taskNumber, 1L)
        requireRecord(taskId == s"task-$next", "non-sequential task ID")
        requireRecord(!tasks.contains(key) && !retiredKeys.contains(key), "reused operation key")
        requireRecord(!reservedEffects.contains(effectKey), "reused effect key")
        val task = AcceptedTask(kind, key, digest, storedEpoch, effectKey, payload, taskId)
        ensureTaskFits(task) match {
          case Left(reason) => throw new IOException(s"Invalid external fixture WAL business payload: $reason")
          case Right(_) => ()
        }
        tasks.put(key, task)
        reservedEffects.add(effectKey)
        taskNumber = next
      case Vector("COMPLETED", key, taskId, storedEpoch, effectKey, result) =>
        val task = tasks.getOrElse(key, throw new IOException("Completion without visible acceptance"))
        requireRecord(task.taskId == taskId && task.ledgerEpoch == storedEpoch &&
          task.effectKey == effectKey, "completion evidence mismatch")
        requireRecord(!completedEffects.contains(effectKey), "duplicate completed effect event")
        val nextBusinessState = validateResult(task, result, businessState)
        completedEffects.put(effectKey, CompletedEffect(key, taskId, storedEpoch, result))
        businessState = nextBusinessState
      case _ => throw new IOException("Unknown external fixture WAL record")
    }

    def recover(): Unit = {
      val size = journal.size()
      var offset = 0L
      var torn = false
      while (offset < size && !torn) {
        val remaining = size - offset
        if (remaining < RecordHeaderBytes) {
          validatePartialHeader(readAt(journal, offset, remaining.toInt), offset)
          torn = true
        } else {
          val header = readAt(journal, offset, RecordHeaderBytes)
          val view = ByteBuffer.wrap(header)
          val magic = view.getInt()
          val length = view.getInt()
          val headerCrc = view.getInt()
          if (magic != RecordMagic || headerCrc != checksum(header, 8))
            throw corrupt(offset, "invalid record header/checksum")
          if (length < 4 || length > MaxBodyBytes) throw corrupt(offset, "invalid record length")
          val recordBytes = RecordHeaderBytes.toLong + length + 4L
          if (remaining < recordBytes) torn = true
          else {
            val body = readAt(journal, offset + RecordHeaderBytes, length)
            val bodyCrc = ByteBuffer.wrap(readAt(journal, offset + RecordHeaderBytes + length, 4)).getInt()
            if (bodyCrc != checksum(body, body.length)) throw corrupt(offset, "invalid record CRC32")
            try applyRecord(decodeBody(body))
            catch { case NonFatal(e) => throw new IOException(s"Invalid fixture WAL record at $offset", e) }
            offset += recordBytes
          }
        }
      }
      if (torn) {
        // Never truncate a complete frame, even when its checksum/schema is bad.
        journal.truncate(offset)
        journal.force(true)
      }
      journal.position(offset)
      if (epochNumber == 0L) append(Vector("EPOCH", "ledger-1"))
    }

    private def append(record: Vector[String]): Unit = {
      val body = encodeBody(record)
      val bytes = ByteBuffer.allocate(RecordHeaderBytes + body.length + 4)
      bytes.put(recordHeader(body.length))
      bytes.put(body)
      bytes.putInt(checksum(body, body.length))
      bytes.flip()
      journal.position(journal.size())
      writeFully(journal, bytes)
      journal.force(true)
      applyRecord(record)
    }

    override def close(): Unit = {
      try lock.release()
      finally journal.close()
    }
  }

  private def requireRecord(condition: Boolean, message: String): Unit =
    if (!condition) throw new IOException(s"Invalid external fixture WAL: $message")

  private def corrupt(offset: Long, message: String): IOException =
    new IOException(s"Corrupt external fixture WAL at $offset: $message")

  private def checksum(bytes: Array[Byte], length: Int): Int = {
    val crc = new CRC32()
    crc.update(bytes, 0, length)
    crc.getValue.toInt
  }

  private def recordHeader(length: Int): Array[Byte] = {
    val header = ByteBuffer.allocate(RecordHeaderBytes)
    header.putInt(RecordMagic)
    header.putInt(length)
    header.putInt(checksum(header.array(), 8))
    header.array()
  }

  private def validatePartialHeader(prefix: Array[Byte], offset: Long): Unit = {
    val magic = ByteBuffer.allocate(4).putInt(RecordMagic).array()
    var index = 0
    while (index < math.min(prefix.length, 4)) {
      if (prefix(index) != magic(index)) throw corrupt(offset, "invalid partial record magic")
      index += 1
    }
    if (prefix.length > 4) {
      val lengthBytes = math.min(prefix.length - 4, 4)
      var partialLength = 0L
      index = 0
      while (index < lengthBytes) {
        partialLength = (partialLength << 8) | (prefix(4 + index) & 0xffL)
        index += 1
      }
      val shift = (4 - lengthBytes) * 8
      val smallest = partialLength << shift
      val largest = smallest + ((1L << shift) - 1L)
      if (smallest > MaxBodyBytes || largest < 4L)
        throw corrupt(offset, "invalid partial record length")
    }
    if (prefix.length >= 8) {
      val expected = recordHeader(ByteBuffer.wrap(prefix).getInt(4))
      index = 8
      while (index < prefix.length) {
        if (prefix(index) != expected(index)) throw corrupt(offset, "invalid partial header checksum")
        index += 1
      }
    }
  }

  private def readAt(channel: FileChannel, offset: Long, length: Int): Array[Byte] = {
    val bytes = ByteBuffer.allocate(length)
    while (bytes.hasRemaining) {
      val read = channel.read(bytes, offset + bytes.position())
      if (read < 0) throw new EOFException("Fixture WAL changed while exclusively locked")
    }
    bytes.array()
  }

  private def writeFully(channel: FileChannel, bytes: ByteBuffer): Unit =
    while (bytes.hasRemaining) channel.write(bytes)

  private def hasOwnershipMarker(directory: Path): Boolean = {
    val marker = directory.resolve(MarkerName)
    try {
      Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS) &&
        Files.isRegularFile(marker, LinkOption.NOFOLLOW_LINKS) &&
        Files.size(marker) == MarkerBytes.length && Arrays.equals(Files.readAllBytes(marker), MarkerBytes)
    } catch { case _: IOException => false }
  }

  private def claimDirectory(directory: Path): Unit = {
    Files.createDirectories(directory)
    if (!Files.isDirectory(directory, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("Fixture directory must be a directory, not a symlink")
    val marker = directory.resolve(MarkerName)
    if (!Files.exists(marker, LinkOption.NOFOLLOW_LINKS)) {
      val entries = Files.newDirectoryStream(directory)
      try {
        if (entries.iterator().hasNext)
          throw new IOException("Refusing to claim a nonempty directory without a fixture marker")
      } finally entries.close()
      val channel = FileChannel.open(marker, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE,
        LinkOption.NOFOLLOW_LINKS)
      try {
        writeFully(channel, ByteBuffer.wrap(MarkerBytes))
        channel.force(true)
      } finally channel.close()
    }
    if (!hasOwnershipMarker(directory)) throw new IOException("Invalid external fixture ownership marker")
  }

  private def openLedger(directory: Path): Ledger = {
    claimDirectory(directory)
    val path = directory.resolve(JournalName)
    if (Files.exists(path, LinkOption.NOFOLLOW_LINKS) && !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS))
      throw new IOException("Fixture WAL must be a regular file, not a symlink")
    val journal = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ,
      StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)
    var lock: FileLock = null
    try {
      lock = journal.tryLock()
      if (lock == null) throw new IOException("Fixture directory is already in use")
      val ledger = new Ledger(directory, journal, lock)
      ledger.recover()
      ledger
    } catch {
      case NonFatal(e) =>
        if (lock != null) closeQuietly(lock)
        closeQuietly(journal)
        throw e
    }
  }

  private def diagnostic(line: String): Unit = {
    System.out.println(line)
    System.out.flush()
  }

  private def closeQuietly(resource: AutoCloseable): Unit =
    try resource.close()
    catch { case NonFatal(_) => () }
}
