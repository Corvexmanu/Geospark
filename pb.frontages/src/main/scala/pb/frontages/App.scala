package pb.frontages

import java.io._
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
import scala.collection.JavaConversions._
import au.com.bytecode.opencsv.CSVWriter
import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.io.File
import scala.io.Source
import java.io.PrintWriter

object App 
{
  
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]");
    
    val sc = new SparkContext(conf)
    val path_CAD = "D:\\WIP\\Project_Frontages_and_Setbacks\\InputDatasets\\Small_Dataset\\Shapefile_output\\Small_CAD_polygon.shp"
    val small_ST = "D:\\WIP\\Project_Frontages_and_Setbacks\\InputDatasets\\Small_Dataset\\Shapefile_output\\Small_ST_line"
    val splitter = FileDataSplitter.CSV
    val storage = StorageLevel.MEMORY_ONLY
    val indexType = IndexType.RTREE
    val grid = GridType.EQUALGRID    
    
    KnnSpatialQuery (sc,path_CAD)
    SpatialRangeQueryWithoutIndex(sc,path_CAD)
  }
  
  
  def SpatialRangeQueryWithoutIndex (sc:SparkContext, path:String){
    val queryEnvelope=new Envelope (138.316576, 138.317247, -35.454158, -35.453624);
    val CadastralRDD = ShapefileReader.readToPolygonRDD(sc, path)  
    val result2 = RangeQuery.SpatialRangeQuery(CadastralRDD, queryEnvelope, false, false); /* The O means consider a point only if it is fully covered by the query window when doing query */
    var polyg = 0
    val writer2 = new PrintWriter(new File("D:\\WIP\\Project_Frontages_and_Setbacks\\Output\\csv\\Write2.txt"))
    writer2.write("POLYGON"+","+"NODE"+","+"X"+","+"Y"+"\r\n")
    Source.fromFile("D:\\WIP\\Project_Frontages_and_Setbacks\\Output\\csv\\Write2.txt").foreach { x => print(x)  }
    result2.collect() foreach {poly =>
      polyg = polyg + 1
      var node = 0
      poly.getCoordinates() foreach {coor =>
        node = node + 1
        writer2.write(polyg + "," + node + ","+ coor.getOrdinate(0) + "," + coor.getOrdinate(1) +"\r\n")
        Source.fromFile("D:\\WIP\\Project_Frontages_and_Setbacks\\Output\\csv\\Write2.txt").foreach { x => print(x) }
                                    }     
                               }            
    writer2.close()
    println("Elkin is ugly")
    }
  
  def KnnSpatialQuery (sc:SparkContext, path:String) {
    val geometryFactory = new GeometryFactory()
    val CadastralRDD = ShapefileReader.readToPolygonRDD(sc, path) 
    val pointObject = geometryFactory.createPoint(new Coordinate(138.312686,-35.452771))
    val K = 1 // K Nearest Neighbors
    val usingIndex = false
    val result = KNNQuery.SpatialKnnQuery(CadastralRDD, pointObject, K, usingIndex)
    var polyg = 0
    val writer = new PrintWriter(new File("D:\\WIP\\Project_Frontages_and_Setbacks\\Output\\csv\\Write.txt"))
    writer.write("POLYGON"+","+"NODE"+","+"X"+","+"Y"+"\r\n")
    Source.fromFile("D:\\WIP\\Project_Frontages_and_Setbacks\\Output\\csv\\Write.txt").foreach { x => print(x)  }
    result foreach {poly =>
      polyg = polyg + 1
      var node = 0
      poly.getCoordinates() foreach {coor =>
        node = node + 1
        writer.write(polyg + "," + node + ","+ coor.getOrdinate(0) + "," + coor.getOrdinate(1) +"\r\n")
        Source.fromFile("D:\\WIP\\Project_Frontages_and_Setbacks\\Output\\csv\\Write.txt").foreach { x => print(x) }
                                    }     
                    }
    writer.close()
    println("Knn Executed")
    } 
}
