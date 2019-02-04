package geospark.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
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

object SimpleApp 
{
  
  def main(args: Array[String]) 
  {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
    
    val sc = new SparkContext(conf)
    val pathPoint = "D:\\GS_libraries\\GeoSpark-master\\core\\src\\test\\resources\\arealm-small.csv"
    val pathRect = "D:\\GS_libraries\\GeoSpark-master\\core\\src\\test\\resources\\zcta510-small.csv"
    val splitter = FileDataSplitter.CSV
    val storage = StorageLevel.MEMORY_ONLY
    val indexType = IndexType.RTREE
    val grid = GridType.EQUALGRID
    
    SpatialRangeQueryWithoutIndex(sc,pathPoint,splitter,storage)
    SpatialRangeQueryWithIndex(sc,pathPoint, splitter,storage,indexType)
    SpatialKNNQueryWithoutIndex(sc,pathPoint,splitter,storage)
    SpatialKNNQueryWithIndex(sc,pathPoint,splitter,storage,indexType)
    SpatialJoinQueryEqualGridWithoutIndex(sc,pathPoint, pathRect, splitter,storage,grid)
    SpatialJoinQueryEqualGridWithIndex(sc,pathPoint, pathRect, splitter,storage, indexType,grid)
    
  }
  
  def SpatialRangeQueryWithoutIndex (sc:SparkContext, path:String, splitter:FileDataSplitter, storage:StorageLevel)
  {
    val queryEnvelope=new Envelope (-90.01, -80.01, 30.01, 40.01);
    val objectRDD = new PointRDD(sc, path,1, splitter,true,storage); /* The O means spatial attribute starts at Column 0 */
    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, false, false).count(); /* The O means consider a point only if it is fully covered by the query window when doing query */
    println(resultSize)
  }
  
  def SpatialRangeQueryWithIndex (sc:SparkContext, path:String, splitter:FileDataSplitter, storage:StorageLevel, indexType:IndexType)
  {
    val queryEnvelope=new Envelope (-90.01, -80.01, 30.01, 40.01);
    val objectRDD = new PointRDD(sc, path,1, splitter,false,storage); /* The O means spatial attribute starts at Column 0 */
    objectRDD.buildIndex(indexType,false); /* Build R-Tree index */
    val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, false, false).count(); 
    println(resultSize)
  }
  
  def SpatialKNNQueryWithoutIndex (sc:SparkContext, path:String, splitter:FileDataSplitter, storage:StorageLevel)
  {
    val fact=new GeometryFactory();
    val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08));
    val objectRDD = new PointRDD(sc, path, 1, splitter,false, storage); /* The O means spatial attribute starts at Column 0 */
    val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5, false);  
    println(resultSize)
  }
  
  def SpatialKNNQueryWithIndex (sc:SparkContext, path:String, splitter:FileDataSplitter, storage:StorageLevel, indexType:IndexType)
  {
    val fact=new GeometryFactory();
    val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08));
    val objectRDD = new PointRDD(sc, path, 1, splitter,false, storage); /* The O means spatial attribute starts at Column 0 */
    objectRDD.buildIndex(indexType,false);
    val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5, true);  
    println(resultSize)
  }
  
  def SpatialJoinQueryEqualGridWithoutIndex (sc:SparkContext, path:String, pathRect:String, splitter:FileDataSplitter, storage:StorageLevel, grid:GridType)
  {
    val objectRDD = new PointRDD(sc, path, 1, splitter,false, storage); /* The O means spatial attribute starts at Column 0 */
    val rectangleRDD = new RectangleRDD(sc, pathRect, 0, splitter,false,storage);
    objectRDD.spatialPartitioning(grid);
    rectangleRDD.spatialPartitioning(objectRDD.grids);
    val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,false,false).count(); 
    println(resultSize)
  }
  
  def SpatialJoinQueryEqualGridWithIndex (sc:SparkContext, path:String, pathRect:String, splitter:FileDataSplitter, storage:StorageLevel, indexType:IndexType, grid:GridType)
  {
    val objectRDD = new PointRDD(sc, path, 1, splitter,false, storage); /* The O means spatial attribute starts at Column 0 */
    val rectangleRDD = new RectangleRDD(sc, pathRect, 0, splitter,false,storage);
    objectRDD.spatialPartitioning(grid);
    objectRDD.buildIndex(indexType,true);
    rectangleRDD.spatialPartitioning(objectRDD.grids);
    val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,true,false).count(); 
    println(resultSize)
  }
}
