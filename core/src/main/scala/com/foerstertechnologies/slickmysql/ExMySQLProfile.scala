package com.foerstertechnologies.slickmysql

import java.util.UUID
import slick.SlickException
import slick.ast._
import slick.compiler.CompilerState
import slick.dbio.{Effect, NoStream}
import slick.jdbc._
import slick.jdbc.meta.MTable
import slick.lifted.PrimaryKey
import slick.util.Logging

import scala.concurrent.ExecutionContext
import scala.reflect.{ClassTag, classTag}

trait ExMySQLProfile extends JdbcProfile with MySQLProfile with Logging { self =>

  override def createUpsertBuilder(node: Insert): InsertBuilder =
    if (useNativeUpsert) new NativeUpsertBuilder(node) else new super.UpsertBuilder(node)
  /*
  override def createQueryBuilder(n: Node, state: CompilerState): QueryBuilder = new QueryBuilder(n, state)

  override def createTableDDLBuilder(table: Table[_]): TableDDLBuilder = new TableDDLBuilder(table)
  override def createModelBuilder(tables: Seq[MTable], ignoreInvalidDefaults: Boolean)(implicit ec: ExecutionContext): JdbcModelBuilder =
    new ExModelBuilder(tables, ignoreInvalidDefaults)
   */
  protected lazy val useNativeUpsert = capabilities contains JdbcCapabilities.insertOrUpdate
  // override protected lazy val useTransactionForUpsert = !useNativeUpsert
  // override protected lazy val useServerSideUpsertReturning = useNativeUpsert

  override val api: API = new API {}

  ///--
  trait API extends super.API {
    // type InheritingTable = self.InheritingTable

    /*
    val Over = window.Over()
    val RowCursor = window.RowCursor

    implicit class AggFuncOver[R: TypedType](aggFunc: agg.AggFuncRep[R]) {
      def over = window.WindowFuncRep[R](aggFunc._parts.toNode(implicitly[TypedType[R]]))
    }
     */
    ///

    implicit def multiUpsertExtensionMethods[U, C[_]](q: Query[_, U, C]): InsertActionComposerImpl[U] =
      new InsertActionComposerImpl[U](compileInsert(q.toNode))

  }

  /*
  trait ByteaPlainImplicits {
    /** NOTE: Array[Byte] maps to `bytea` instead of `byte ARRAY` */
    implicit val getByteArray = new GetResult[Array[Byte]] {
      def apply(pr: PositionedResult) = pr.nextBytes()
    }
    implicit val getByteArrayOption = new GetResult[Option[Array[Byte]]] {
      def apply(pr: PositionedResult) = pr.nextBytesOption()
    }
    implicit val setByteArray = new SetParameter[Array[Byte]] {
      def apply(v: Array[Byte], pp: PositionedParameters) = pp.setBytes(v)
    }
    implicit val setByteArrayOption = new SetParameter[Option[Array[Byte]]] {
      def apply(v: Option[Array[Byte]], pp: PositionedParameters) = pp.setBytesOption(v)
    }
  }*/

  /*************************************************************************
    *                 for aggregate and window function support
    *************************************************************************/
  /*
  class QueryBuilder(tree: Node, state: CompilerState) extends super.QueryBuilder(tree, state) {
    import slick.util.MacroSupport.macroSupportInterpolation
    override def expr(n: Node, skipParens: Boolean = false) = n match {
      case agg.AggFuncExpr(func, params, orderBy, filter, distinct, forOrdered) =>
        if (func == Library.CountAll) b"${func.name}"
        else {
          b"${func.name}("
          if (distinct) b"distinct "
          b.sep(params, ",")(expr(_, true))
          if (orderBy.nonEmpty && !forOrdered) buildOrderByClause(orderBy)
          b")"
        }
        if (orderBy.nonEmpty && forOrdered) { b" within group ("; buildOrderByClause(orderBy); b")" }
        if (filter.isDefined) { b" filter ("; buildWhereClause(filter); b")" }
      case window.WindowFuncExpr(aggFuncExpr, partitionBy, orderBy, frameDef) =>
        expr(aggFuncExpr)
        b" over ("
        if(partitionBy.nonEmpty) { b" partition by "; b.sep(partitionBy, ",")(expr(_, true)) }
        if(orderBy.nonEmpty) buildOrderByClause(orderBy)
        frameDef.map {
          case (mode, start, Some(end)) => b" $mode between $start and $end"
          case (mode, start, None)      => b" $mode $start"
        }
        b")"
      case _ => super.expr(n, skipParens)
    }
  }
   */

  /***********************************************************************
    *                          for upsert support
    ***********************************************************************/
  class NativeUpsertBuilder(ins: Insert) extends super.InsertBuilder(ins) {
    /* NOTE: pk defined by using method `primaryKey` and pk defined with `PrimaryKey` can only have one,
             here we let table ddl to help us ensure this. */
    private lazy val (nonPkAutoIncSyms, insertingSyms) = syms.toSeq.partition { s =>
      s.options.contains(ColumnOption.AutoInc) && !(s.options contains ColumnOption.PrimaryKey)
    }

    private lazy val insertNames = insertingSyms.map { fs => quoteIdentifier(fs.name) }
    val unquoted = insertNames.map(_.tail.init)

    override def buildInsert: InsertBuilderResult = {
      val columns = insertNames.mkString(",")
      val namedParameters = insertNames.map(_ => "?").mkString(",")
      val pairs = insertNames.map(column => s"$column = values($column)").mkString(",")

      val upsert = s"""insert into $tableName ($columns) values ($namedParameters)
                      |on duplicate key update
                      |  $pairs;
                      |""".stripMargin

      new InsertBuilderResult(table, upsert, syms)
    }

    override def transformMapping(n: Node) =
      reorderColumns(n, insertingSyms ++ nonPkAutoIncSyms ++ nonPkAutoIncSyms ++ nonPkAutoIncSyms)
  }

  protected class InsertActionComposerImpl[U](override val compiled: CompiledInsert)
      extends super.CountingInsertActionComposerImpl[U](compiled) {

    /** Upsert a batch of records - insert rows whose primary key is not present in
      * the table, and update rows whose primary key is present.. */
    def insertOrUpdateAll(values: Iterable[U]): ProfileAction[MultiInsertResult, NoStream, Effect.Write] =
      if (useNativeUpsert)
        new MultiInsertOrUpdateAction(values)
      else
        throw new IllegalStateException(
          "Cannot insertOrUpdateAll in without native upsert capability. " +
            "Instead use DBIO.sequence(values.map(query.insertOrUpdate))"
        )

    class MultiInsertOrUpdateAction(values: Iterable[U])
        extends SimpleJdbcProfileAction[MultiInsertResult]("MultiInsertOrUpdateAction", Vector(compiled.upsert.sql)) {

      private def tableHasPrimaryKey: Boolean =
        List(compiled.upsert, compiled.checkInsert, compiled.updateInsert)
          .filter(_ != null)
          .exists(artifacts =>
            artifacts.ibr.table.profileTable.asInstanceOf[Table[_]].primaryKeys.nonEmpty
              || artifacts.ibr.fields.exists(_.options.contains(ColumnOption.PrimaryKey))
          )

      if (!tableHasPrimaryKey)
        throw new SlickException("InsertOrUpdateAll is not supported on a table without PK.")

      override def run(ctx: Backend#Context, sql: Vector[String]) =
        nativeUpsert(values, sql.head)(ctx.session)

      protected def nativeUpsert(
          values: Iterable[U],
          sql: String
        )(
          implicit session: Backend#Session
        ): MultiInsertResult =
        preparedInsert(sql, session) { st =>
          st.clearParameters()
          for (value <- values) {
            compiled.upsert.converter
              .set(value, st)
            st.addBatch()
          }
          val counts = st.executeBatch()
          retManyBatch(st, values, counts)
        }
    }
  }

  /***********************************************************************
    *                          for codegen support
    ***********************************************************************/
  private var mySQLTypeToScala = Map.empty[String, ClassTag[_]]

  /** NOTE: used to support code gen */
  def bindMySQLTypeToScala(mySQLType: String, scalaType: ClassTag[_]) = {
    logger.info(s"\u001B[36m >>> binding $mySQLType -> $scalaType \u001B[0m")
    mySQLTypeToScala.synchronized {
      val existed = mySQLTypeToScala.get(mySQLType)
      if (existed.isDefined && !existed.get.equals(scalaType))
        logger.warn(
          s"\u001B[31m >>> DUPLICATED binding for $mySQLType - existed: ${existed.get}, new: $scalaType !!! \u001B[36m If it's expected, pls ignore it.\u001B[0m"
        )
      mySQLTypeToScala += (mySQLType -> scalaType)
    }
  }

  {
    bindMySQLTypeToScala("binary(16)", classTag[UUID])
    // bindMySQLTypeToScala("text", classTag[String])
    bindMySQLTypeToScala("bool", classTag[Boolean])
  }

  class ExModelBuilder(mTables: Seq[MTable], ignoreInvalidDefaults: Boolean)(implicit ec: ExecutionContext)
      extends super.ModelBuilder(mTables, ignoreInvalidDefaults) {
    override def jdbcTypeToScala(jdbcType: Int, typeName: String = ""): ClassTag[_] = {
      logger.info(s"[info]\u001B[36m jdbcTypeToScala - jdbcType $jdbcType, typeName: $typeName \u001B[0m")
      mySQLTypeToScala.get(typeName).getOrElse(super.jdbcTypeToScala(jdbcType, typeName))
    }
  }

  /***********************************************************************
    *                          for inherit support
    ***********************************************************************/

  /*
  trait InheritingTable { sub: Table[_] =>
    val inherited: Table[_]
  }

  class TableDDLBuilder(table: Table[_]) extends super.TableDDLBuilder(table) {
    override protected val columns: Iterable[ColumnDDLBuilder] = {
      (if(table.isInstanceOf[InheritingTable]) {
        val hColumns = table.asInstanceOf[InheritingTable].inherited.create_*.toSeq.map(_.name.toLowerCase)
        table.create_*.filterNot(s => hColumns.contains(s.name.toLowerCase))
      } else table.create_*)
        .map(fs => createColumnDDLBuilder(fs, table))
    }
    override protected val primaryKeys: Iterable[PrimaryKey] = {
      if(table.isInstanceOf[InheritingTable]) {
        val hTable = table.asInstanceOf[InheritingTable].inherited
        val hPrimaryKeys = hTable.primaryKeys.map(pk => PrimaryKey(table.tableName + "_" + pk.name, pk.columns))
        hTable.create_*.find(_.options.contains(ColumnOption.PrimaryKey))
          .map(s => PrimaryKey(table.tableName + "_PK", IndexedSeq(Select(tableNode, s))))
          .map(Iterable(_) ++ hPrimaryKeys ++ table.primaryKeys)
          .getOrElse(hPrimaryKeys ++ table.primaryKeys)
      } else table.primaryKeys
    }

    override protected def createTable(checkNotExists: Boolean): String = {
      if(table.isInstanceOf[InheritingTable]) {
        val hTable = table.asInstanceOf[InheritingTable].inherited
        val hTableNode = hTable.toNode.asInstanceOf[TableExpansion].table.asInstanceOf[TableNode]
        s"${super.createTable(checkNotExists)} inherits (${quoteTableName(hTableNode)})"
      } else super.createTable(checkNotExists)
    }
  }

 */
}

object ExMySQLProfile extends ExMySQLProfile
