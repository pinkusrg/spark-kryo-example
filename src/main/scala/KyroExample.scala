import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object KyroExample extends App {

  case class Person(name: String, age: Int)

  val conf = new SparkConf()
    .setAppName("kyroExample")
    .setMaster("local[*]")
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("setWarnUnregisteredClasses","true")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(
      Array(classOf[Person],classOf[Array[Person]],
      Class.forName("org.apache.spark.internal.io.FileCommitProtocol$TaskCommitMessage"))
    )

  val sparkContext = new SparkContext(conf)
  val personList: Array[Person] = (1 to 99999).map(value => Person("p"+value, value)).toArray

  val rddPerson: RDD[Person] = sparkContext.parallelize(personList,5)
  val evenAgePerson: RDD[Person] = rddPerson.filter(_.age % 2 == 0)

  evenAgePerson.persist(StorageLevel.MEMORY_ONLY_SER)

  evenAgePerson.take(50).foreach(person=>println(person.name,person.age))

  Thread.sleep(200000)
}

