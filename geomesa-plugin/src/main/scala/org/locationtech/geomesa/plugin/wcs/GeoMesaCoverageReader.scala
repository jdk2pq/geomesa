package org.locationtech.geomesa.plugin.wcs

import java.awt.Rectangle

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.{AbstractGridCoverage2DReader, AbstractGridFormat}
import org.geotools.coverage.grid.{GridCoverage2D, GridEnvelope2D}
import org.geotools.factory.Hints
import org.geotools.geometry.GeneralEnvelope
import org.geotools.util.Utilities
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.opengis.parameter.GeneralParameterValue

import scala.collection.JavaConversions._

object GeoMesaCoverageReader {
  val GeoServerDateFormat = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
  val DefaultDateString = GeoServerDateFormat.print(new DateTime(DateTimeZone.forID("UTC")))
  val FORMAT = """accumulo://(.*):(.*)@(.*)/(.*)#geohash=(.*)#resolution=([0-9]*)#timeStamp=(.*)#rasterName=(.*)#zookeepers=([^#]*)(?:#auths=)?(.*)$""".r
}

import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageReader._

class GeoMesaCoverageReader(val url: String, hints: Hints) extends AbstractGridCoverage2DReader() with Logging {

  logger.debug(s"""creating coverage reader for url "${url.replaceAll(":.*@", ":********@").replaceAll("#auths=.*","#auths=********")}"""")
  val FORMAT(user, password, instanceId, table, geohash, resolutionStr, timeStamp, rasterName, zookeepers, authtokens) = url
  logger.debug(s"extracted user $user, password ********, instance id $instanceId, table $table, zookeepers $zookeepers, auths ********")

  coverageName = table + ":" + rasterName

  this.crs = AbstractGridFormat.getDefaultCRS
  this.originalEnvelope = new GeneralEnvelope(Array(-180.0, -90.0), Array(180.0, 90.0))
  this.originalEnvelope.setCoordinateReferenceSystem(this.crs)
  this.originalGridRange = new GridEnvelope2D(new Rectangle(0, 0, 1024, 512))
  this.coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(this.hints)

  val coverageStoreParams = mapAsJavaMap(Map[java.lang.String, java.io.Serializable](
    "instanceId" -> instanceId,
    "zookeepers" -> zookeepers,
    "user" -> user,
    "password" -> password,
    "tableName" -> table,
    "auths" -> authtokens,
    "password" -> password
  ))
  val coverageStore = AccumuloCoverageStore(coverageStoreParams)

  /**
   * Default implementation does not allow a non-default coverage name
   * @param coverageName
   * @return
   */
  override protected def checkName(coverageName: String) = {
    Utilities.ensureNonNull("coverageName", coverageName)
    true
  }

  override def getCoordinateReferenceSystem = this.crs

  override def getCoordinateReferenceSystem(coverageName: String) = this.getCoordinateReferenceSystem

  override def getFormat = new GeoMesaCoverageFormat

  def getGeohashPrecision = resolutionStr.toInt

  def read(parameters: Array[GeneralParameterValue]): GridCoverage2D = {
    val params = new GeoMesaCoverageQueryParams(parameters)
    val image = coverageStore.getChunk(geohash, getGeohashPrecision)

    /**
     * Included for when mosaicing and final key structure are utilized
     *
     * val image = getChunk(geohash, params.resolution.getOrElse(getGeohashPrecision))
     * val chunks = getChunks(geohash, getGeohashPrecision, None, params.bbox)
     * val image = mosaicGridCoverages(chunks, env = params.env)
     * this.coverageFactory.create(coverageName, image, params.env)
     */
    //    val imageChoice = setReadParams(params.overviewPolicy, new ImageReadParam(), new GeneralEnvelope(params.envelope), params.dim)

    this.coverageFactory.create(coverageName, image, params.envelope)
  }
}