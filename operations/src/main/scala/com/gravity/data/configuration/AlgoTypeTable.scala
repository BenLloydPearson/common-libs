package com.gravity.data.configuration

import com.gravity.utilities.Settings2
import com.gravity.utilities.cache.PermaCacher

import scala.slick.lifted.ProvenShape

/**
 * Created by agrealish14 on 7/17/16.
 */
trait AlgoTypeTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class AlgoTypeTable(tag: Tag) extends Table[AlgoTypeRow](tag, "AlgoType") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    override def *  : ProvenShape[AlgoTypeRow] = (id, name) <> ((AlgoTypeRow.apply _).tupled, AlgoTypeRow.unapply)
  }

  val AlgoTypeTable = scala.slick.lifted.TableQuery[AlgoTypeTable]
}

trait RecommenderTypeTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class RecommenderTypeTable(tag: Tag) extends Table[RecommenderTypeRow](tag, "RecommenderType") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    override def *  : ProvenShape[RecommenderTypeRow] = (id, name) <> ((RecommenderTypeRow.apply _).tupled, RecommenderTypeRow.unapply)
  }

  val RecommenderTypeTable = scala.slick.lifted.TableQuery[RecommenderTypeTable]
}

trait AlgoTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class AlgoTable(tag: Tag) extends Table[AlgoRow](tag, "Algo") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    def algoTypeId = column[Long]("algoTypeId")

    override def *  : ProvenShape[AlgoRow] = (id, name, algoTypeId) <> (AlgoRow.tupled, AlgoRow.unapply)
  }

  val AlgoTable = scala.slick.lifted.TableQuery[AlgoTable]
}

trait StrategyTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class StrategyTable(tag: Tag) extends Table[StrategyRow](tag, "Strategy") {

    def id = column[Long]("id", O.PrimaryKey, O.AutoInc)

    def name = column[String]("name")

    override def *  : ProvenShape[StrategyRow] = (id, name) <> (StrategyRow.tupled, StrategyRow.unapply)
  }

  val StrategyTable = scala.slick.lifted.TableQuery[StrategyTable]
}

trait RecommenderTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class RecommenderTable(tag: Tag) extends Table[RecommenderRow](tag, "Recommender") {

    def id = column[Long]("id", O.PrimaryKey)

    def name = column[String]("name")

    def isDefault = column[Boolean]("isDefault")

    def typeId = column[Long]("typeId")

    def strategyId = column[Long]("strategyId")

    override def *  : ProvenShape[RecommenderRow] = (id, name, isDefault, typeId, strategyId) <> (RecommenderRow.tupled, RecommenderRow.unapply)
  }

  val RecommenderTable = scala.slick.lifted.TableQuery[RecommenderTable]
}

trait RecommenderAlgoPriorityTable {
  this: ConfigurationDatabase =>

  import driver.simple._

  class RecommenderAlgoPriorityTable(tag: Tag) extends Table[RecommenderAlgoPriorityRow](tag, "RecommenderAlgoPriority") {

    def recommenderId = column[Long]("recommenderId")

    def algoId = column[Long]("algoId")

    def priority = column[Long]("priority")

    def pk = primaryKey("pk_RecommenderAlgoPriorityRow", (recommenderId, algoId))

    override def *  : ProvenShape[RecommenderAlgoPriorityRow] = (recommenderId, algoId, priority) <> (RecommenderAlgoPriorityRow.tupled, RecommenderAlgoPriorityRow.unapply)
  }

  val RecommenderAlgoPriorityTable = scala.slick.lifted.TableQuery[RecommenderAlgoPriorityTable]
}


trait PlacementConfigQuerySupport {
  this: ConfigurationQuerySupport =>

  private val d = driver

  import d.simple._

  def insert(row: AlgoTypeRow): AlgoTypeRow = database withSession {
    implicit session => {
      val id = (algoTypeTable returning algoTypeTable.map(_.id)) += row
      row.copy(id = id)
    }
  }

  def allAlgoTypesWithoutCaching: List[AlgoTypeRow] = readOnlyDatabase withSession { implicit s: Session =>
    algoTypeTable.list(s)
  }

  def allAlgoTypes: List[AlgoTypeRow] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.allAlgoTypes",
    allAlgoTypesWithoutCaching,
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def insert(row: RecommenderTypeRow): RecommenderTypeRow = database withSession {
    implicit session => {
      val id = (recommenderTypeTable returning recommenderTypeTable.map(_.id)) += row
      row.copy(id = id)
    }
  }

  def allRecommenderTypesWithoutCaching: List[RecommenderTypeRow] = readOnlyDatabase withSession { implicit s: Session =>
    recommenderTypeTable.list(s)
  }

  def allRecommenderTypes: List[RecommenderTypeRow] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.allRecommenderTypes",
    allRecommenderTypesWithoutCaching,
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def insert(row: AlgoRow): AlgoRow = database withSession {
    implicit session => {
      val id = (algoTable returning algoTable.map(_.id)) += row
      row.copy(id = id)
    }
  }

  def allAlgosWithoutCaching: List[Algo] = readOnlyDatabase withSession { implicit s: Session =>

    val query = for {
      (a, at) <- algoTable join algoTypeTable on (_.algoTypeId === _.id)
    } yield (a, at)

    val q = query.map(res => {
      (res._1, res._2)
    })

    q.list(s).map( (res: (AlgoRow, AlgoTypeRow)) => {
      Algo(res._1.id, res._1.name, res._2)
    })
  }

  def allAlgos: List[Algo] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.allAlgos",
    allAlgosWithoutCaching,
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def insert(row: RecommenderAlgoPriorityRow): RecommenderAlgoPriorityRow = database withSession {
    implicit session => {
      recommenderAlgoPriorityTable += row
      row
    }
  }

  def algoPrioritiesByRecommenderIdWithoutCaching(recommenderId:Long): List[AlgoPriority] = readOnlyDatabase withSession { implicit s: Session =>

    val query = for {
      rao <- recommenderAlgoPriorityTable if rao.recommenderId === recommenderId
      a <- algoTable if rao.algoId === a.id
      at <- algoTypeTable if a.algoTypeId === at.id
    } yield (rao, a, at)

    val q = query.map(res => {
      (res._1, res._2, res._3)
    })

    q.list(s).map( (res: (RecommenderAlgoPriorityRow, AlgoRow, AlgoTypeRow)) => {
      AlgoPriority(res._1.priority, Algo(res._2.id, res._2.name, res._3))
    })
  }

  def algoPrioritiesByRecommenderId(recommenderId:Long): List[AlgoPriority] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.algoPrioritiesByRecommenderId." + recommenderId,
    algoPrioritiesByRecommenderIdWithoutCaching(recommenderId),
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def insert(row: StrategyRow): StrategyRow = database withSession {
    implicit session => {
      val id = (strategyTable returning strategyTable.map(_.id)) += row
      row.copy(id = id)
    }
  }

  def allStrategiesWithoutCaching: List[StrategyRow] = readOnlyDatabase withSession { implicit s: Session =>
    strategyTable.list(s)
  }

  def allStrategies: List[StrategyRow] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.allStrategies",
    allStrategiesWithoutCaching,
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def strategyWithoutCaching(strategyId:Long): Option[Strategy] = readOnlyDatabase withSession { implicit s: Session =>

    val query = strategyTable.filter(_.id === strategyId)

    val q = query.map(res => {
      res
    })

    val res: List[StrategyRow] = q.list(s)

    if(!res.isEmpty) {

      val s = res.head

      Some(Strategy(s.id, s.name))
    } else {

      None
    }

    /*
    // Our JDBC driver prevents this "firstOption" syntax from working?
    q.firstOption(s) match {
      case Some(res) => {
        //Some(Strategy(res.id, res.name, algosByStrategyId(strategyId)))
        Some(Strategy(res.id, res.name))
      }
      case None => None
    }
    */

    //Some(Strategy(1, ""))

  }

  def strategy(strategyId:Long): Option[Strategy] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.strategyById." + strategyId,
    strategyWithoutCaching(strategyId),
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def insert(row: RecommenderRow): RecommenderRow = database withSession {
    implicit session => {
      val id = (recommenderTable returning recommenderTable.map(_.id)) += row
      row.copy(id = id)
    }
  }

  def allRecommendersWithoutCaching: List[RecommenderConfig] = readOnlyDatabase withSession { implicit s: Session =>

    val query = for {
      r <- recommenderTable
      rt <- recommenderTypeTable if r.typeId === rt.id
      s <- strategyTable if r.strategyId === s.id
    } yield (r, rt, s)

    val q = query.map(res => {
      (res._1, res._2, res._3)
    })

    q.list(s).map( (res: (RecommenderRow, RecommenderTypeRow, StrategyRow)) => {
      RecommenderConfig(res._1.id, res._1.name, res._1.isDefault, res._2, Strategy(res._3.id, res._3.name), algoPrioritiesByRecommenderId(res._1.id))
    })

  }

  def allRecommenders: List[RecommenderConfig] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.allRecommenders",
    allRecommendersWithoutCaching,
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def recommenderWithoutCaching(recommenderId:Long): Option[RecommenderConfig] = readOnlyDatabase withSession { implicit s: Session =>

    val query = for {
      r <- recommenderTable if r.id === recommenderId
      rt <- recommenderTypeTable if r.typeId === rt.id
      s <- strategyTable if r.strategyId === s.id
    } yield (r, rt, s)

    val q = query.map(res => {
      (res._1, res._2, res._3)
    })

    val res: List[RecommenderConfig] = q.list(s).map( (res: (RecommenderRow, RecommenderTypeRow, StrategyRow)) => {
      RecommenderConfig(res._1.id, res._1.name, res._1.isDefault, res._2, Strategy(res._3.id, res._3.name), algoPrioritiesByRecommenderId(res._1.id))
    })

    if(res.nonEmpty) {

      res.headOption

    } else {

      None
    }

  }

  def recommender(recommenderId:Long): Option[RecommenderConfig] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.recommender."+recommenderId,
    recommenderWithoutCaching(recommenderId),
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )

  def recommenderByNameWithoutCaching(name:String): Option[RecommenderConfig] = readOnlyDatabase withSession { implicit s: Session =>

    val query = for {
      r <- recommenderTable if r.name === name
      rt <- recommenderTypeTable if r.typeId === rt.id
      s <- strategyTable if r.strategyId === s.id
    } yield (r, rt, s)

    val q = query.map(res => {
      (res._1, res._2, res._3)
    })

    val res: List[RecommenderConfig] = q.list(s).map( (res: (RecommenderRow, RecommenderTypeRow, StrategyRow)) => {
      RecommenderConfig(res._1.id, res._1.name, res._1.isDefault, res._2, Strategy(res._3.id, res._3.name), algoPrioritiesByRecommenderId(res._1.id))
    })

    if(res.nonEmpty) {

      res.headOption

    } else {

      None
    }

  }

  def recommenderByName(name:String): Option[RecommenderConfig] = PermaCacher.getOrRegister(
    "PlacementConfigQuerySupport.recommenderByName."+name,
    recommenderByNameWithoutCaching(name),
    PlacementConfigQuerySupport.placementConfigReloadSeconds.getOrElse(permacacherDefaultTTL)
  )
}

object PlacementConfigQuerySupport {
  val placementConfigReloadSeconds: Option[Long] = Settings2.getLong("recommendation.widget.allAolDlSettingsReloadSeconds")
}
