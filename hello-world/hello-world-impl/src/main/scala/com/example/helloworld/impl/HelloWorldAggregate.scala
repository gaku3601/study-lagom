package com.example.helloworld.impl

import play.api.libs.json.Json
import play.api.libs.json.Format
import java.time.LocalDateTime

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.cluster.sharding.typed.scaladsl._
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.Effect
import akka.persistence.typed.scaladsl.EventSourcedBehavior
import akka.persistence.typed.scaladsl.ReplyEffect
import com.lightbend.lagom.scaladsl.persistence.AggregateEvent
import com.lightbend.lagom.scaladsl.persistence.AggregateEventTag
import com.lightbend.lagom.scaladsl.persistence.AkkaTaggerAdapter
import com.lightbend.lagom.scaladsl.playjson.JsonSerializer
import com.lightbend.lagom.scaladsl.playjson.JsonSerializerRegistry
import play.api.libs.json._

import scala.collection.immutable.Seq

/**
  * これにより、イベントソースの動作が提供されます。状態[[HelloWorld State]]があり、
  * 挨拶の内容が格納されています（例： "Hello"）。
  *
  * イベントソースのエンティティは、コマンドを送信することで相互作用します。
  * この集約は、greetingを変更するために使用される[[UseGreetingMessage]]コマンドと、
  * コマンドで指定された名前にgreetingを返す読み取り専用コマンドである[[Hello]]コマンドの2つのコマンドをサポートします。
  *
  * コマンドはイベントに変換され、永続化されるのはイベントです。各イベントにはイベントハンドラーが登録されており、
  * イベントハンドラーはイベントを現在の状態に適用するだけです。これは、イベントが最初に作成されたときに行われ、
  * データベースからアグリゲートがロードされたときにも行われます。各イベントが再生され、アグリゲートの状態が再作成されます。
  *
  * この集約は、1つのイベント[[GreetingMessageChanged]]イベントを定義します。
  * このイベントは、[[UseGreetingMessage]]コマンドを受け取ったときに発行されます。
  */
object HelloWorldBehavior {

  /**
    * シャーディング[[EntityContext]]を指定すると、この関数は集約のAkka [[Behavior]]を生成します。
    */ 
  def create(entityContext: EntityContext[HelloWorldCommand]): Behavior[HelloWorldCommand] = {
    val persistenceId: PersistenceId = PersistenceId(entityContext.entityTypeKey.name, entityContext.entityId)

    create(persistenceId)
      .withTagger(
        // Lagomで入力されたAkka Persistenceを使用するには、Lagom互換の方法でイベントにタグを付ける必要があります。
        // これにより、Lagom ReadSideProcessorsとTopicProducersがイベントストリームを見つけて追跡できるようになります。
        AkkaTaggerAdapter.fromLagom(entityContext, HelloWorldEvent.Tag)
      )

  }
  /*
   * このメソッドは、Akka Clusterに完全に依存しない単体テストを記述するために抽出されています。
   */
  private[impl] def create(persistenceId: PersistenceId) = EventSourcedBehavior
      .withEnforcedReplies[HelloWorldCommand, HelloWorldEvent, HelloWorldState](
        persistenceId = persistenceId,
        emptyState = HelloWorldState.initial,
        commandHandler = (cart, cmd) => cart.applyCommand(cmd),
        eventHandler = (cart, evt) => cart.applyEvent(evt)
      )
}

/**
  * 集計の現在の状態。
  */
case class HelloWorldState(message: String, timestamp: String) {
  def applyCommand(cmd: HelloWorldCommand): ReplyEffect[HelloWorldEvent, HelloWorldState] =
    cmd match {
      case x: Hello              => onHello(x)
      case x: UseGreetingMessage => onGreetingMessageUpgrade(x)
    }

  def applyEvent(evt: HelloWorldEvent): HelloWorldState =
    evt match {
      case GreetingMessageChanged(msg) => updateMessage(msg)
    }
  private def onHello(cmd: Hello): ReplyEffect[HelloWorldEvent, HelloWorldState] =
    Effect.reply(cmd.replyTo)(Greeting(s"$message, ${cmd.name}!"))

  private def onGreetingMessageUpgrade(
    cmd: UseGreetingMessage
  ): ReplyEffect[HelloWorldEvent, HelloWorldState] =
    Effect
      .persist(GreetingMessageChanged(cmd.message))
      .thenReply(cmd.replyTo) { _ =>
        Accepted
      }

  private def updateMessage(newMessage: String) =
    copy(newMessage, LocalDateTime.now().toString)
}

object HelloWorldState {

  /**
    * 初期状態。これは、検出するスナップショット状態がない場合に使用されます。
    */
  def initial: HelloWorldState = HelloWorldState("Hello", LocalDateTime.now.toString)

  /**
    * [[EventSourcedBehavior]]インスタンス（別名Aggregates）は、Akkaクラスター内のシャーディングされた
    * アクターで実行されます。アクターをシャーディングしてクラスター全体に分散する場合、
    * 各アグリゲートは、名前とシャーディングされたアクターが受信できるコマンドのタイプを指定するタイプキーの下で名前空間に割り当てられます。
    */
  val typeKey = EntityTypeKey[HelloWorldCommand]("HelloWorldAggregate")

  /**
    * hello状態のフォーマット。
    *
    * 永続化されたエンティティは、設定された数のイベントごとにスナップショットを取得します。
    * これは、状態がデータベースに保存されることを意味します。これにより、集計が読み込まれたときに、
    * スナップショット以降のイベントだけを再生する必要はありません。したがって、JSON形式は、
    * データベースとの間で保管するときにシリアライズおよびデシリアライズできるように宣言する必要があります。
    */
  implicit val format: Format[HelloWorldState] = Json.format
}

/**
  * このインターフェイスは、HelloWorldAggregateがサポートするすべてのイベントを定義します。
  */
sealed trait HelloWorldEvent extends AggregateEvent[HelloWorldEvent] {
  def aggregateTag: AggregateEventTag[HelloWorldEvent] = HelloWorldEvent.Tag
}

object HelloWorldEvent {
  val Tag: AggregateEventTag[HelloWorldEvent] = AggregateEventTag[HelloWorldEvent]
}

/**
  * greetingメッセージの変化を表すイベント。
  */
case class GreetingMessageChanged(message: String) extends HelloWorldEvent

object GreetingMessageChanged {

  /**
   * greetingメッセージ変更イベントのフォーマット。
   *
   * イベントはデータベースから保存およびロードされるため、
   * シリアル化および逆シリアル化できるようにJSON形式を宣言する必要があります。
   */
  implicit val format: Format[GreetingMessageChanged] = Json.format
}

/**
  * This is a marker trait for commands.
  * これらは、replyToフィールドを処理できるAkkaのJacksonサポートを使用してシリアル化します。
  * (see application.conf)
  */
trait HelloWorldCommandSerializable

/**
  * このインターフェイスは、HelloWorldAggregateがサポートするすべてのコマンドを定義します。
  */
sealed trait HelloWorldCommand
    extends HelloWorldCommandSerializable

/**
  * greetingメッセージを切り替えるコマンド。
  * 応答タイプは[[Confirmation]]であり、このコマンドによって
  * 発行されたすべてのイベントが正常に永続化されると、呼び出し元に返信されます。
  */
case class UseGreetingMessage(message: String, replyTo: ActorRef[Confirmation])
    extends HelloWorldCommand

/**
  * 現在のgreetingメッセージを使用している人に挨拶するコマンド。
  *
  * 返信タイプは文字列で、その人に言うメッセージが含まれます。
  */
case class Hello(name: String, replyTo: ActorRef[Greeting])
    extends HelloWorldCommand

final case class Greeting(message: String)

object Greeting {
  implicit val format: Format[Greeting] = Json.format
}

sealed trait Confirmation

case object Confirmation {
  implicit val format: Format[Confirmation] = new Format[Confirmation] {
    override def reads(json: JsValue): JsResult[Confirmation] = {
      if ((json \ "reason").isDefined)
        Json.fromJson[Rejected](json)
      else
        Json.fromJson[Accepted](json)
    }

    override def writes(o: Confirmation): JsValue = {
      o match {
        case acc: Accepted => Json.toJson(acc)
        case rej: Rejected => Json.toJson(rej)
      }
    }
  }
}

sealed trait Accepted extends Confirmation

case object Accepted extends Accepted {
  implicit val format: Format[Accepted] =
    Format(Reads(_ => JsSuccess(Accepted)), Writes(_ => Json.obj()))
}

case class Rejected(reason: String) extends Confirmation

object Rejected {
  implicit val format: Format[Rejected] = Json.format
}

/**
  * 永続化とリモート処理の両方で使用されるAkkaシリアル化では、シリアル化または
  * 逆シリアル化されたすべての型に対してシリアライザを登録する必要があります。
  * Akkaメッセージに任意のシリアライザーを使用することは可能ですが、
  * そのままの状態でLagomはこのレジストリ抽象化を介してJSONをサポートします。
  *
  * シリアライザはここに登録され、アプリケーションローダーでLagomに提供されます。
  */
object HelloWorldSerializerRegistry extends JsonSerializerRegistry {
  override def serializers: Seq[JsonSerializer[_]] = Seq(
    // state and events can use play-json, but commands should use jackson because of ActorRef[T] (see application.conf)
    JsonSerializer[GreetingMessageChanged],
    JsonSerializer[HelloWorldState],
    // the replies use play-json as well
    JsonSerializer[Greeting],
    JsonSerializer[Confirmation],
    JsonSerializer[Accepted],
    JsonSerializer[Rejected]
  )
}
