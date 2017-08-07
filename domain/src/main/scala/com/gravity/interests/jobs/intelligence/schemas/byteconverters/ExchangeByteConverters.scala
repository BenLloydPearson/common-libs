package com.gravity.interests.jobs.intelligence.schemas.byteconverters

import com.gravity.hbase.schema.{ComplexByteConverter, PrimitiveInputStream, PrimitiveOutputStream}
import com.gravity.interests.jobs.intelligence._
import com.gravity.valueclasses.ValueClassesForDomain._

/*

         ▒▒
         ██
       ██████
     ██████████
     ██░░██░░██
     ██████████
       ██████
       ░░░░░░      It's gonna be fun on the bun!
       ██████

*/

trait ExchangeByteConverters {
  this: SchemaTypes.type =>

  implicit object ExchangeKeyConverter extends ComplexByteConverter[ExchangeKey] {
    override def write(key: ExchangeKey, output: PrimitiveOutputStream) {
      output.writeLong(key.exchangeId)
    }

    override def read(input: PrimitiveInputStream): ExchangeKey = {
      ExchangeKey(input.readLong())
    }
  }

  implicit object ExchangeSiteKeyConverter extends ComplexByteConverter[ExchangeSiteKey] {
    override def write(key: ExchangeSiteKey, output: PrimitiveOutputStream) {
      output.writeObj(key.siteKey)
      output.writeObj(key.exchangeKey)
    }

    override def read(input: PrimitiveInputStream): ExchangeSiteKey = {
      val siteKey = input.readObj[SiteKey]
      val exchangeKey = input.readObj[ExchangeKey]
      ExchangeSiteKey(exchangeKey, siteKey)
    }
  }

  implicit object ExchangeTypeConverter extends ComplexByteConverter[ExchangeType.Type] {
    def write(data: ExchangeType.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.i)
    }

    def read(input: PrimitiveInputStream): ExchangeType.Type = ExchangeType.parseOrDefault(input.readByte())
  }

  implicit object ExchangeGuidConverter extends ComplexByteConverter[ExchangeGuid] {
    def write(data: ExchangeGuid, output: PrimitiveOutputStream) {
      output.writeUTF(data.raw)
    }

    def read(input: PrimitiveInputStream): ExchangeGuid = input.readUTF().asExchangeGuid
  }

  implicit object ExchangeStatusConverter extends ComplexByteConverter[ExchangeStatus.Type] {
    val instance: ExchangeStatusConverter.type = this

    def write(data: ExchangeStatus.Type, output: PrimitiveOutputStream) {
      output.writeByte(data.id)
    }

    def read(input: PrimitiveInputStream): ExchangeStatus.Type = ExchangeStatus.parseOrDefault(input.readByte())
  }

  implicit object ClicksUnlimitedGoalByteConverter extends ComplexByteConverter[ClicksUnlimitedGoal] {
    val version = 1

    def write(data: ClicksUnlimitedGoal, output: PrimitiveOutputStream) {
      output.writeInt(version)
    }
    def read(input: PrimitiveInputStream): ClicksUnlimitedGoal = {
      val readVersion = input.readInt()

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ClicksUnlimitedGoal", version, version, version))

      new ClicksUnlimitedGoal()
    }
  }

  implicit object ClicksReceivedMonthlyGoalByteConverter extends ComplexByteConverter[ClicksReceivedMonthlyGoal] {
    val version = 1

    def write(data: ClicksReceivedMonthlyGoal, output: PrimitiveOutputStream) {
      output.writeInt(version)
      output.writeLong(data.clickGoal)
    }
    def read(input: PrimitiveInputStream): ClicksReceivedMonthlyGoal = {
      val readVersion = input.readInt()

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("ClicksReceivedMonthlyGoal", version, version, version))

      ClicksReceivedMonthlyGoal(input.readLong())
    }
  }

  implicit object NoExchangeThrottleByteConverter extends ComplexByteConverter[NoExchangeThrottle] {
    val version = 1

    def write(data: NoExchangeThrottle, output: PrimitiveOutputStream) {
      output.writeInt(version)
    }
    def read(input: PrimitiveInputStream): NoExchangeThrottle = {
      val readVersion = input.readInt()

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("NoExchangeThrottle", version, version, version))

      new NoExchangeThrottle()
    }
  }

  implicit object MonthlyMaxReceiveSendRatioThrottleByteConverter extends ComplexByteConverter[MonthlyMaxReceiveSendRatioThrottle] {
    val version = 1

    def write(data: MonthlyMaxReceiveSendRatioThrottle, output: PrimitiveOutputStream) {
      output.writeInt(version)
      output.writeDouble(data.ratio)
    }
    def read(input: PrimitiveInputStream): MonthlyMaxReceiveSendRatioThrottle = {
      val readVersion = input.readInt()

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("MonthlyMaxReceiveSendRatioThrottle", version, version, version))

      MonthlyMaxReceiveSendRatioThrottle(input.readDouble())
    }
  }

  implicit object MonthlySendReceiveBufferThrottleByteConverter extends ComplexByteConverter[MonthlySendReceiveBufferThrottle] {
    val version = 1

    def write(data: MonthlySendReceiveBufferThrottle, output: PrimitiveOutputStream) {
      output.writeInt(version)
      output.writeLong(data.buffer)
    }
    def read(input: PrimitiveInputStream): MonthlySendReceiveBufferThrottle = {
      val readVersion = input.readInt()

      if (readVersion != version)
        throw new RuntimeException(UnsupportedVersionExcepion.buildMessage("MonthlySendReceiveBufferThrottle", version, version, version))

      MonthlySendReceiveBufferThrottle(input.readLong())
    }
  }

}
