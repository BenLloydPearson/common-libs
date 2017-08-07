package com.gravity.interests.jobs.intelligence

import com.gravity.interests.jobs.intelligence.operations.{ArticleRecoData, SampleNestedEvent, SampleEvent}
import com.gravity.utilities.eventlogging.FieldValueRegistry
import com.gravity.utilities.{grvfields, BaseScalaTest}
import org.joda.time.DateTime
import org.junit.Assert._
import com.gravity.utilities.grvfields._
import scalaz.{Failure, Success}

/**
 * Created by cstelzmuller on 8/5/15.
 */
object SampleEventTest {
  val nestedEvent: SampleNestedEvent = new SampleNestedEvent(
    "TestString",
    2,
    3L,
    4.0F,
    4.0,
    true,
    List("String Abba", "String Bubba"),
    List(7, 8, 9),
    List(110, 111, 112),
    List(120.1F, 120.2F, 120.3F),
    List(150.1, 150.2, 150.3),
    List(false, false, false),
    new DateTime(),
    List(new DateTime(), new DateTime().minusDays(20), new DateTime().minusYears(2)),
    ArticleRecoData.empty
  )

  val event: SampleEvent = SampleEvent(
    "TestString",
    1,
    2L,
    1.0F,
    1.0,
    true,
    List("String One", "String Two"),
    List(3, 4, 5),
    List(10, 11, 12),
    List(20.1F, 20.2F, 20.3F),
    List(50.1, 50.2, 50.3),
    List(false, false, false),
    new DateTime(),
    List(new DateTime(), new DateTime().minusDays(10), new DateTime().minusYears(1)),
    nestedEvent,
    List(nestedEvent, nestedEvent, nestedEvent),
    "samplebytes".getBytes,
    Option("someString"),
    Option(null),
    "TestUnencodedString([]'')!+&=\\",
    List("Unenco\"dedStri/ngOne", "Unenco\rde\ndStringTwo"),
    List("listsamplebytes".getBytes, "list2samplebytes".getBytes)
  )
}

class SampleEventTest extends BaseScalaTest {
  import com.gravity.domain.FieldConverters._
  import SampleEventTest._

  test("field read write") {
    val recordString = grvfields.toDelimitedFieldString(event)
    val recordBytes = grvfields.toBytes(event)
    val recordAvro = grvfields.toAvroRecord(event)
    println(recordAvro.toString)

    FieldValueRegistry.getInstanceFromAvroRecord[SampleEvent](recordAvro) match {
      case Success(s: SampleEvent) => {
        assertEquals(recordString, s.toDelimitedFieldString)
        assertArrayEquals(recordBytes, s.toBytes)
        assertEquals(recordAvro, s.toAvroRecord)
        assertEquals(s.seOptionStringTwo, None)
      }
      case Failure(fails) => assertTrue(fails.toString(), false)
    }
  }

  test("avro schema") {
    val seSchema = grvfields.toAvroSchema[SampleEvent]
    println(seSchema.toString(true))

  }
}
