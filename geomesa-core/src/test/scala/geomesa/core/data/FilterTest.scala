package geomesa.core.data

import geomesa.core.iterators.{TestData, SpatioTemporalIntersectingIteratorTest}
import geomesa.core.iterators.TestData._
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.geotools.data.Query
import org.joda.time._
import geomesa.core.index.IndexSchema
import org.opengis.feature.simple.SimpleFeature
import org.apache.accumulo.core.client.Connector

@RunWith(classOf[JUnitRunner])
class FilterTest extends Specification {

  val stiit = new SpatioTemporalIntersectingIteratorTest

  val fullDataFeatures = fullData.map(createSF)
  val fullDataConnector = TestData.setupMockAccumuloTable(fullData, "fullData")

  val hugeDataFeatures = hugeData.map(createSF)
  val hugeDataConnector = TestData.setupMockAccumuloTable(hugeData, "hugeData")

  val predicates = List("INTERSECTS", "DISJOINT", "CROSSES", "CONTAINS", "OVERLAPS", "WITHIN")

  // Predicates which are downright silly
  val silly = List("TOUCHES", "EQUALS")


  // Worthless predicates: predicats which don't return results against 'fullData'.
  val untested = List("CONTAINS", "TOUCHES", "EQUALS", "DISJOINT", "CROSSES")

  // These predicates are *not* working presently.
  val failingPredicates = List("OVERLAPS", "WITHIN")

  val tails = List("(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))")

//  val tails = List("(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
//                   "(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23))) AND (attr2 like '2nd___')")

  val dtFilter: Interval = IndexSchema.everywhen
  val int2 = new Interval(new DateTime("2010-07-01T00:00:00.000Z"), new DateTime("2010-07-31T00:00:00.000Z"))

  val ints: List[Interval] = List(dtFilter, int2, null)

  val basicFilters: List[String] = for {
    p <- predicates
    t <- tails
    i <- ints
  } yield red(s"$p$t", dt(i))


  val geoms = List("(geomesa_index_geometry, POLYGON ((45 23, 48 23, 48 27, 45 27, 45 23)))",
    "(geomesa_index_geometry, POLYGON ((41 28, 42 28, 42 29, 41 29, 41 28)))",
    "(geomesa_index_geometry, POLYGON ((44 23, 46 23, 46 25, 44 25, 44 23)))")

  val moreGeomFilters = for {
    p <- predicates
    g <- geoms
  } yield s"$p$g"

  val oneAndGeoms = for {
    p1 <- predicates
    p2 <- predicates
    g1 <- geoms
    g2 <- geoms if g2 != g1
  } yield s"$p1$g1 AND $p2$g2"

  val oneOrGeoms = for {
    p1 <- predicates
    p2 <- predicates
    g1 <- geoms
    g2 <- geoms if g2 != g1
  } yield s"$p1$g1 OR $p2$g2"

  val filters: List[String] = basicFilters ++ moreGeomFilters ++ oneAndGeoms :+ "INCLUDE"  //++ oneOrGeoms
  //val filters = oneOrGeoms
  // RELATE, DWITHIN, BEYOND, BBOX

//  filters.map{ fs => s"Filter $fs should work the same" should {
//    "in Mock GeoMesa and directly for the fullDataFeatures" in {
//        compare(fs, fullDataFeatures, "fullData", fullDataConnector)
//      }
//    }
//  }

  //basicFilters.map{ fs => s"Filter $fs should work the same" should {

  filters.map{ fs => s"Filter $fs should work the same" should {
    "in Mock GeoMesa and directly for the hugeDataFeatures" in {
      compare(fs, hugeDataFeatures, "hugeData", hugeDataConnector)
      }
    }
  }

  def compare(fs: String, features: List[SimpleFeature], target: String, conn: Connector) = {
    val bs = conn.createBatchScanner(target, TEST_AUTHORIZATIONS, 5)
    val filter = ECQL.toFilter(fs)

    val q = new Query(TestData.featureType.getTypeName, filter)

    val filteredNumber: Int = features.count(filter.evaluate)
    val mockNumber: Int = stiit.runQuery(q, bs, false)

    println(s"Filter against $target: $fs filtered: $filteredNumber mockNumber: $mockNumber")
    filteredNumber mustEqual mockNumber
  }


  def dt(i: Interval): Option[String] =
    Option(i) map {int => s"(geomesa_index_start_time between '${int.getStart}' AND '${int.getEnd}')"
  }

  def red(f: String, og: Option[String]) = og match {
    case Some(g) => s"$f AND $g"
    case None => f
  }
}