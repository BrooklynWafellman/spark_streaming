import scalafx.Includes._
import scalafx.animation.{Animation, KeyFrame, Timeline}
import scalafx.application.JFXApp3
import scalafx.application.JFXApp3.PrimaryStage
import scalafx.collections.ObservableBuffer
import scalafx.geometry.{Insets, Pos}
import scalafx.scene.Scene
import scalafx.scene.chart.{LineChart, NumberAxis, XYChart}
import scalafx.scene.control.Label
import scalafx.scene.layout.{BorderPane, VBox}
import scalafx.util.Duration

import scala.util.Random

object Interface extends JFXApp3 {

    override def start(): Unit = {

      // ---- Titre en haut à gauche ----
      val title = new Label("Mon Dashboard Streaming") {
        style = "-fx-font-size: 20px; -fx-font-weight: bold;"
      }
      val titleBox = new VBox(title) {
        padding = Insets(10)
        alignment = Pos.TopLeft
      }

      // ---- Graphique au centre ----
      val xAxis = new NumberAxis()
      val yAxis = new NumberAxis()
      xAxis.label = "Temps"
      yAxis.label = "Valeur"

      val series1 = new XYChart.Series[Number, Number] { name = "Résultat A" }
      val series2 = new XYChart.Series[Number, Number] { name = "Résultat B" }

      val lineChart = new LineChart[Number, Number](xAxis, yAxis) {
        title = "Évolution en temps réel"
        data = ObservableBuffer(series1.delegate, series2.delegate)
      }

      // ---- Layout global ----
      val rootPane = new BorderPane {
        top = titleBox
        center = lineChart
        padding = Insets(10)
      }

      stage = new PrimaryStage {
        title = "Streaming Dashboard"
        scene = new Scene(900, 600) {
          root = rootPane
        }
      }

      // ---- Refresh périodique (ex: toutes les 2 secondes) ----
      var tick = 0
      val refreshInterval = Duration(2000) // à mettre dans ton fichier conf plus tard

      val timeline = new Timeline {
        cycleCount = Animation.Indefinite
        keyFrames = Seq(
          KeyFrame(refreshInterval, onFinished = _ => {
            tick += 1
            refreshChart(series1, series2, tick)
          })
        )
      }
      timeline.play()
    }

    // ---- Fonction de refresh : à remplacer par ta lecture réelle (fichier/table Spark) ----
    def refreshChart(series1: XYChart.Series[Number, Number], series2: XYChart.Series[Number, Number], tick: Int): Unit = {
      // Exemple : ici tu iras lire ton sink (parquet/table/etc.) au lieu de Random
      series1.data() += XYChart.Data[Number, Number](tick, Random.nextInt(100))
      series2.data() += XYChart.Data[Number, Number](tick, Random.nextInt(100))

      // Garder seulement les N derniers points affichés (ex: 20)
      val maxPoints = 20
      if (series1.data().size > maxPoints) series1.data().remove(0)
      if (series2.data().size > maxPoints) series2.data().remove(0)
    }
  }

