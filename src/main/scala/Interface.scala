import scalafx.Includes._
import scalafx.application.{JFXApp3, Platform}
import scalafx.application.JFXApp3.PrimaryStage
import scalafx.beans.property.StringProperty
import scalafx.collections.ObservableBuffer
import scalafx.geometry.{Insets, Pos}
import scalafx.scene.Scene
import scalafx.scene.chart.{BarChart, CategoryAxis, LineChart, NumberAxis, XYChart}
import scalafx.scene.control.{Button, Label, TableColumn, TableView}
import scalafx.scene.layout.{BorderPane, GridPane, HBox, Region, VBox}
import scalafx.stage.FileChooser
import scalafx.stage.FileChooser.ExtensionFilter
import scalafx.util.StringConverter

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File
import java.nio.file.{Files, StandardCopyOption}
import java.time.{Instant, ZoneId}
import java.time.format.DateTimeFormatter
import java.util.concurrent.{Executors, TimeUnit}
import javafx.beans.value.{ChangeListener, ObservableValue}
import javafx.scene.layout.Priority

object Interface extends JFXApp3 {

  // ---- Config (mêmes chemins que le Consumer) ----
  private val config = ConfigFactory.load("config/consumer.conf")
  private val baseDbPath = config.getString("app.baseDbPath")

  // Dossier surveillé par le Consumer (= output_path du producer), pour l'upload manuel d'images
  private val configProd = ConfigFactory.load("config/producer.conf")
  private val inputPath   = configProd.getString("app.output_path")

  // Fenêtre glissante affichée sur chaque graphique
  private val windowMinutes = 5L
  private val refreshSeconds = 10L

  // Palette générale du dashboard
  private val bgColor    = "#eef1f5"
  private val cardColor  = "#ffffff"
  private val accentColor = "#2f6fed"

  // ---- Spark en lecture batch (statique), distinct de la session streaming du Consumer ----
  private lazy val spark = SparkSession.builder()
    .appName("Interface")
    .master("local[*]")
    .config("spark.log.level", "WARN")
    .getOrCreate()

  // Schéma explicite = pas d'inférence -> pas d'échec quand le dossier n'a encore aucun fichier parquet.
  // C'est le même schéma "bronze" que celui écrit par Consumer.scala dans baseDbPath.
  private val bronzeSchema = StructType(Seq(
    StructField("path", StringType),
    StructField("ingest_time", TimestampType),
    StructField("original", BinaryType),
    StructField("original_shape", ArrayType(IntegerType)),
    StructField("processed", BinaryType),
    StructField("processed_shape", ArrayType(IntegerType)),
    StructField("mean_r", DoubleType),
    StructField("mean_g", DoubleType),
    StructField("mean_b", DoubleType),
    StructField("predicted_result", FloatType),
    StructField("real_result", FloatType),
  ))

  // Lecture batch tolérante : si le dossier n'existe pas encore ou est vide, renvoie un DataFrame vide au lieu de planter
  private def readParquetSafe(path: String, schema: StructType): DataFrame = {
    if (!new File(path).exists()) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
    } else {
      spark.read.schema(schema).parquet(path)
    }
  }

  // Lecture batch de la base bronze, déjà filtrée sur la fenêtre glissante affichée.
  // Aucune notion de watermark ici : on relit tout ce qui est physiquement sur disque
  // au moment du refresh, donc pas de latence d'attente comme avec un stream + watermark.
  private def readBaseWindowed(): DataFrame =
    readParquetSafe(baseDbPath, bronzeSchema)
      .filter(col("ingest_time") >= lit(cutoffTimestamp()))

  private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault())

  // Libellé métier des classes 0/1 pour tous les graphiques
  // Convention : 1 = Avec Masque (dossier "WithMask"), 0 = Sans Masque. Doit rester synchronisée avec Consumer.scala.
  private def classLabel(label: Int): String = if (label == 1) "Avec Masque" else "Sans Masque"

  private def axisFormatter(): StringConverter[Number] = new StringConverter[Number] {
    override def toString(t: Number): String = timeFormatter.format(Instant.ofEpochSecond(t.longValue()))
    override def fromString(s: String): Number = 0
  }

  // Enveloppe visuelle "carte" commune à tous les blocs du dashboard
  private def card(content: scalafx.scene.Node*): VBox = new VBox(content: _*) {
    spacing = 10
    padding = Insets(12)
    style =
      s"""-fx-background-color: $cardColor;
         |-fx-background-radius: 10;
         |-fx-border-radius: 10;
         |-fx-border-color: derive(-fx-base, -12%);
         |-fx-effect: dropshadow(gaussian, rgba(0,0,0,0.12), 10, 0, 0, 2);""".stripMargin
  }

  // ---- Graphiques en ligne (prédictions / réel) : échelle Y fixe 0-20 ----
  private def makeLineChart(chartTitle: String, yLabel: String): LineChart[Number, Number] = {
    val xAxis = new NumberAxis()
    xAxis.label = "Temps"
    xAxis.tickLabelFormatter = axisFormatter()
    xAxis.forceZeroInRange = false
    val yAxis = new NumberAxis(0, 20, 2)
    yAxis.label = yLabel
    yAxis.autoRanging = false
    val chart = new LineChart[Number, Number](xAxis, yAxis)
    chart.title = chartTitle
    chart.createSymbols = false
    chart.animated = false
    chart.style = "-fx-background-color: transparent;"
    chart
  }

  private def getOrCreateSeries(chart: LineChart[Number, Number], seriesName: String): XYChart.Series[Number, Number] = {
    chart.data().find(_.name.value == seriesName) match {
      case Some(s) => s
      case None =>
        val s = new XYChart.Series[Number, Number]()
        s.name = seriesName
        chart.data().add(s.delegate)
        s
    }
  }

  private def setSeriesData(series: XYChart.Series[Number, Number], points: Seq[(Long, Double)]): Unit = {
    series.data() = ObservableBuffer(points.map { case (x, y) => XYChart.Data[Number, Number](x, y) }: _*)
  }

  // ---- Bar chart (couleur moyenne par classe réelle) ----
  // Chaque barre est coloriée selon son propre canal (nuances de rouge/vert/bleu), pas selon la série,
  // pour que R soit toujours en rouge, G en vert, B en bleu, avec une nuance différente par classe réelle.
  private val colorDark0 = Map("R" -> "#8b0000", "G" -> "#1b5e20", "B" -> "#0d1a66")
  private val colorLight1 = Map("R" -> "#ff8c69", "G" -> "#8bd88b", "B" -> "#87cefa")

  private def colorFor(category: String, label: Int): String =
    if (label == 0) colorDark0.getOrElse(category, "#888888") else colorLight1.getOrElse(category, "#888888")

  private def makeBarChart(chartTitle: String, yLabel: String): BarChart[String, Number] = {
    val xAxis = new CategoryAxis()
    xAxis.label = "Canal"
    xAxis.categories = ObservableBuffer("R", "G", "B")
    val yAxis = new NumberAxis()
    yAxis.label = yLabel
    val chart = new BarChart[String, Number](xAxis, yAxis)
    chart.title = chartTitle
    chart.animated = false
    chart.legendVisible = false // la légende par série n'a plus de sens : la couleur dépend du canal, pas de la série
    chart.style = "-fx-background-color: transparent;"
    chart
  }

  private def getOrCreateBarSeries(chart: BarChart[String, Number], seriesName: String): XYChart.Series[String, Number] = {
    chart.data().find(_.name.value == seriesName) match {
      case Some(s) => s
      case None =>
        val s = new XYChart.Series[String, Number]()
        s.name = seriesName
        chart.data().add(s.delegate)
        s
    }
  }

  // Colore chaque barre dès que son nœud JavaFX est créé (le nœud n'existe qu'après le prochain layout)
  private def setBarSeriesDataColored(series: XYChart.Series[String, Number], label: Int, points: Seq[(String, Double)]): Unit = {
    val dataPoints = points.map { case (category, value) =>
      val d = XYChart.Data[String, Number](category, value)
      d.delegate.nodeProperty().addListener(new ChangeListener[javafx.scene.Node] {
        override def changed(obs: ObservableValue[_ <: javafx.scene.Node], oldNode: javafx.scene.Node, newNode: javafx.scene.Node): Unit = {
          if (newNode != null) newNode.setStyle(s"-fx-bar-fill: ${colorFor(category, label)};")
        }
      })
      d
    }
    series.data() = ObservableBuffer(dataPoints: _*)
  }

  private def legendSwatch(color: String): Region = new Region {
    prefWidth = 14
    prefHeight = 14
    style = s"-fx-background-color: $color; -fx-background-radius: 3;"
  }

  private def legendEntry(color: String, text: String): HBox = new HBox(legendSwatch(color), new Label(text)) {
    spacing = 6
    alignment = Pos.CenterLeft
  }

  private def makeColorLegend(): HBox = new HBox(
    legendEntry(colorDark0("R"), s"${classLabel(0)} (foncé)"),
    legendEntry(colorLight1("R"), s"${classLabel(1)} (clair)")
  ) {
    spacing = 24
    alignment = Pos.Center
    padding = Insets(4, 0, 0, 0)
  }

  // ---- Table (top des résolutions) ----
  class ShapeRow(resolutionInit: String, countInit: Long) {
    val resolution: StringProperty = StringProperty(resolutionInit)
    val count: StringProperty = StringProperty(countInit.toString)
  }

  private def makeShapeTable(): TableView[ShapeRow] = {
    val resolutionCol = new TableColumn[ShapeRow, String]("Résolution") {
      cellValueFactory = { _.value.resolution }
      sortable = false
    }
    val countCol = new TableColumn[ShapeRow, String]("Nombre d'images") {
      cellValueFactory = { _.value.count }
      sortable = false
      style = "-fx-alignment: CENTER-RIGHT;"
    }
    val table = new TableView[ShapeRow]() {
      style = "-fx-font-size: 13px; -fx-background-color: transparent;"
      placeholder = new Label("En attente de données...")
    }
    table.columns ++= Seq(resolutionCol, countCol)
    table.columnResizePolicy = javafx.scene.control.TableView.CONSTRAINED_RESIZE_POLICY
    VBox.setVgrow(table, Priority.ALWAYS)
    table
  }

  override def start(): Unit = {

    val title = new Label("Dashboard Streaming - Détection de masque") {
      style = s"-fx-font-size: 22px; -fx-font-weight: bold; -fx-text-fill: $accentColor;"
    }
    val subtitle = new Label("Statistiques sur les 5 dernières minutes") {
      style = "-fx-font-size: 12px; -fx-text-fill: #666666;"
    }
    val titleTexts = new VBox(title, subtitle) { spacing = 2 }

    val uploadButton = new Button("+ Uploader une image") {
      style =
        s"""-fx-background-color: $accentColor;
           |-fx-text-fill: white;
           |-fx-font-weight: bold;
           |-fx-background-radius: 6;
           |-fx-padding: 8 16;
           |-fx-cursor: hand;""".stripMargin
      onAction = _ => {
        val fileChooser = new FileChooser {
          title = "Choisir une image à ajouter au flux"
          extensionFilters += new ExtensionFilter("Images", Seq("*.png", "*.jpg", "*.jpeg", "*.bmp"))
        }
        val selected = fileChooser.showOpenDialog(stage.delegate)
        if (selected != null) {
          try {
            // On récupère le dossier parent de l'image choisie (ex: .../WithMask/xxx.png -> "WithMask")
            // pour la ranger dans le même sous-dossier de classe côté input_path du Consumer.
            val classFolder = Option(selected.getParentFile).map(_.getName).getOrElse("unknown")
            val destDir = new File(inputPath, classFolder)
            if (!destDir.exists()) destDir.mkdirs()
            val destFile = new File(destDir, s"${System.currentTimeMillis()}_${selected.getName}")
            Files.copy(selected.toPath, destFile.toPath, StandardCopyOption.REPLACE_EXISTING)
            println(s"Image copiée vers ${destFile.getAbsolutePath}")
          } catch {
            case e: Exception => println(s"Erreur upload image: ${e.getMessage}")
          }
        }
      }
    }

    val titleBox = new HBox(titleTexts, uploadButton) {
      padding = Insets(16, 20, 16, 20)
      alignment = Pos.CenterLeft
      style = s"-fx-background-color: $cardColor;"
      HBox.setHgrow(titleTexts, Priority.ALWAYS)
    }

    val predictedChart = makeLineChart("Prédictions (label 0/1)", "Nombre d'images")
    val realChart      = makeLineChart("Réel (label 0/1)", "Nombre d'images")
    val colorChart     = makeBarChart("Couleur moyenne par classe réelle", "Valeur moyenne du canal (0-255)")
    val colorLegend    = makeColorLegend()
    val shapeTable      = makeShapeTable()

    val shapeTableLabel = new Label("Top 10 des résolutions d'images") {
      style = "-fx-font-size: 14px; -fx-font-weight: bold;"
    }

    val predictedCard = card(predictedChart)
    val realCard       = card(realChart)
    val colorCard      = card(colorChart, colorLegend)
    val shapeCard      = card(shapeTableLabel, shapeTable)
    VBox.setVgrow(predictedChart, Priority.ALWAYS)
    VBox.setVgrow(realChart, Priority.ALWAYS)
    VBox.setVgrow(colorChart, Priority.ALWAYS)
    VBox.setVgrow(shapeTable, Priority.ALWAYS)

    val grid = new GridPane {
      hgap = 16
      vgap = 16
      padding = Insets(16)
    }
    grid.add(predictedCard, 0, 0)
    grid.add(realCard, 1, 0)
    grid.add(colorCard, 0, 1)
    grid.add(shapeCard, 1, 1)
    Seq[javafx.scene.Node](predictedCard, realCard, colorCard, shapeCard).foreach { c =>
      GridPane.setHgrow(c, Priority.ALWAYS)
      GridPane.setVgrow(c, Priority.ALWAYS)
    }

    val rootPane = new BorderPane {
      top = titleBox
      center = grid
      style = s"-fx-background-color: $bgColor;"
    }

    stage = new PrimaryStage {
      title = "Streaming Dashboard"
      scene = new Scene(1300, 850) {
        root = rootPane
      }
    }

    // ---- Refresh périodique en dehors du thread JavaFX ----
    val scheduler = Executors.newSingleThreadScheduledExecutor()
    val task: Runnable = () => {
      try {
        // Une seule lecture/filtre de la base bronze, réutilisée pour les 4 agrégations :
        // évite de relire et refiltrer baseDbPath 4 fois par cycle de refresh.
        val windowed = readBaseWindowed().cache()
        try {
          refreshPredicted(predictedChart, windowed)
          refreshReal(realChart, windowed)
          refreshColor(colorChart, windowed)
          refreshShape(shapeTable, windowed)
        } finally {
          windowed.unpersist()
        }
      } catch {
        case e: Exception =>
          // ex: pas encore de fichiers écrits par le Consumer au tout début
          println(s"Refresh dashboard: ${e.getMessage}")
      }
    }
    scheduler.scheduleAtFixedRate(task, 0, refreshSeconds, TimeUnit.SECONDS)

    stage.onCloseRequest = _ => {
      scheduler.shutdownNow()
      spark.stop()
    }
  }

  private def cutoffTimestamp(): java.sql.Timestamp =
    java.sql.Timestamp.from(Instant.now().minusSeconds(windowMinutes * 60))

  private def withEpochSeconds(df: DataFrame): DataFrame =
    df.withColumn("window_start_sec", col("window.start").cast("long"))

  // Toutes les agrégations ci-dessous sont calculées en BATCH sur les données actuellement
  // présentes dans baseDbPath (paramètre "windowed", déjà filtré sur les 5 dernières minutes).
  // Pas de watermark, pas de mode "append" streaming : le résultat reflète toujours l'état
  // réel du disque au moment du refresh, donc pas de latence d'attente.

  private def refreshPredicted(chart: LineChart[Number, Number], windowed: DataFrame): Unit = {
    val rows = withEpochSeconds(
      windowed
        .groupBy(
          window(col("ingest_time"), "20 seconds"),
          col("predicted_result")
        )
        .agg(count("*").as("count"))
    )
      .select("window_start_sec", "predicted_result", "count")
      .collect()

    val byLabel = rows.groupBy(_.getAs[Float]("predicted_result"))
    Platform.runLater {
      Seq(0f, 1f).foreach { label =>
        val points = byLabel.getOrElse(label, Array.empty[Row])
          .map(r => (r.getAs[Long]("window_start_sec"), r.getAs[Long]("count").toDouble))
          .sortBy(_._1).toSeq
        setSeriesData(getOrCreateSeries(chart, classLabel(label.toInt)), points)
      }
    }
  }

  private def refreshReal(chart: LineChart[Number, Number], windowed: DataFrame): Unit = {
    val rows = withEpochSeconds(
      windowed
        .groupBy(
          window(col("ingest_time"), "20 seconds"),
          col("real_result")
        )
        .agg(count("*").as("count"))
    )
      .select("window_start_sec", "real_result", "count")
      .collect()

    val byLabel = rows.groupBy(_.getAs[Float]("real_result"))
    Platform.runLater {
      Seq(0f, 1f).foreach { label =>
        val points = byLabel.getOrElse(label, Array.empty[Row])
          .map(r => (r.getAs[Long]("window_start_sec"), r.getAs[Long]("count").toDouble))
          .sortBy(_._1).toSeq
        setSeriesData(getOrCreateSeries(chart, classLabel(label.toInt)), points)
      }
    }
  }

  // Barchart : moyenne pondérée de chaque canal (R/G/B), par classe réelle, sur toute la fenêtre de 5 min
  private def refreshColor(chart: BarChart[String, Number], windowed: DataFrame): Unit = {
    val rows = windowed
      .groupBy("real_result")
      .agg(
        sum("mean_r").as("total_r"),
        sum("mean_g").as("total_g"),
        sum("mean_b").as("total_b"),
        count("*").as("total_count")
      )
      .collect()

    val byLabel = rows.map(r => r.getAs[Float]("real_result") -> r).toMap

    Platform.runLater {
      Seq(0f, 1f).foreach { label =>
        val values: Seq[(String, Double)] = byLabel.get(label) match {
          case Some(r) =>
            val totalCount = r.getAs[Long]("total_count")
            if (totalCount > 0) {
              Seq(
                "R" -> r.getAs[Double]("total_r") / totalCount,
                "G" -> r.getAs[Double]("total_g") / totalCount,
                "B" -> r.getAs[Double]("total_b") / totalCount
              )
            } else Seq("R" -> 0.0, "G" -> 0.0, "B" -> 0.0)
          case None => Seq("R" -> 0.0, "G" -> 0.0, "B" -> 0.0)
        }
        setBarSeriesDataColored(getOrCreateBarSeries(chart, s"réel=${label.toInt}"), label.toInt, values)
      }
    }
  }

  // Table : top 10 des résolutions sur la fenêtre de 5 min, triées par nombre d'images décroissant
  private def refreshShape(table: TableView[ShapeRow], windowed: DataFrame): Unit = {
    val rows = windowed
      .withColumn("shape_label", concat_ws("x", transform(col("original_shape"), c => c.cast("string"))))
      .groupBy("shape_label")
      .agg(count("*").as("count"))
      .select("shape_label", "count")
      .collect()

    val totals = rows
      .map(r => r.getAs[String]("shape_label") -> r.getAs[Long]("count"))
      .sortBy(-_._2).take(10)

    Platform.runLater {
      table.items = ObservableBuffer(totals.map { case (shape, count) => new ShapeRow(shape, count) }: _*)
    }
  }
}