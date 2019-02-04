package pb.frontages

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.datasyslab.geospark.formatMapper.shapefileParser.ShapefileReader
import org.datasyslab.geospark.spatialOperator.RangeQuery; 
import org.datasyslab.geospark.spatialRDD.PointRDD;
import com.vividsolutions.jts.geom.Envelope;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.IndexType;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Coordinate;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.enums.GridType;

object App 
{
  
  def main(args: Array[String]) 
  {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
    
    val sc = new SparkContext(conf)
    val path_CAD = "D:\\WIP\\Project_Frontages_and_Setbacks\\InputDatasets\\Small_Dataset\\Shapefile_output\\Small_CAD_polygon.shp"
    val small_ST = "D:\\WIP\\Project_Frontages_and_Setbacks\\InputDatasets\\Small_Dataset\\Shapefile_output\\Small_ST_line"
    val splitter = FileDataSplitter.CSV
    val storage = StorageLevel.MEMORY_ONLY
    val indexType = IndexType.RTREE
    val grid = GridType.EQUALGRID
    
    
    SpatialRangeQueryWithoutIndex(sc,path_CAD)
    KnnSpatialQuery (sc,path_CAD)
  }
  
  def SpatialRangeQueryWithoutIndex (sc:SparkContext, path:String)
  {
    val queryEnvelope=new Envelope (-35.45149572, -35.44764606, 138.3153426, 138.3195838);
    val CadastralRDD = ShapefileReader.readToPolygonRDD(sc, path)  
    val resultSize = RangeQuery.SpatialRangeQuery(CadastralRDD, queryEnvelope, false, false); /* The O means consider a point only if it is fully covered by the query window when doing query */
    println(resultSize.count())
    println(resultSize)
    CadastralRDD.saveAsGeoJSON("D:\\WIP\\Project_Frontages_and_Setbacks\\InputDatasets\\Small_Dataset\\GeoJSON_output\\temp")
    println("File Saved")
  }
  
  def KnnSpatialQuery (sc:SparkContext, path:String)
  {
    val geometryFactory = new GeometryFactory()
    val CadastralRDD = ShapefileReader.readToPolygonRDD(sc, path) 
    val pointObject = geometryFactory.createPoint(new Coordinate(-35.452771, 138.312686))
    val K = 1 // K Nearest Neighbors
    val usingIndex = false
    val resultSize = KNNQuery.SpatialKnnQuery(CadastralRDD, pointObject, K, usingIndex)
    println(resultSize)
    println(pointObject)
    println("KNN executed")
  }
  
}
