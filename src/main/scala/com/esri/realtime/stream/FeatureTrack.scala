package com.esri.realtime.stream

import java.util
import java.util.function.Predicate
import java.util.{Collections, Comparator, NoSuchElementException}
import com.esri.arcgis.st.{Feature, FeatureSchema}
import scala.util.control.Breaks._

case class NumEntriesPredicate[Feature](var numEntries: Int) extends Predicate[Feature] with Serializable {
  override def test(feature: Feature): Boolean = {
    val testResult = numEntries > 0
    numEntries = if (testResult) numEntries - 1 else 0
    testResult
  }
}

case class FeatureTrack(purger: FeatureTrackPurger)(implicit val featureSchema: FeatureSchema) extends Serializable {
  val track: util.SortedSet[Feature] = new util.TreeSet[Feature](FeatureOrder.chronological)
  def iterator: util.Iterator[Feature] = track.iterator
  def size: Int = track.size
  def isEmpty: Boolean = track.isEmpty

  def contains(feature: Feature): Boolean = {
    try {
      return track.contains(feature)
    } catch {
      case _: ClassCastException =>
    }
    false
  }

  def add(feature: Feature): Boolean = {
    if (!contains(feature) && track.add(feature)) {
      purger.purge(this)
      return true
    }
    false
  }

  def remove(feature: Feature): Boolean = {
    try {
      return track.remove(feature)
    } catch {
      case _: ClassCastException =>
    }
    false
  }

  def clear(): Unit = track.clear()

  def latest: Option[Feature] = Option(
    try {
      track.last
    } catch {
      case _: NoSuchElementException => null
    }
  )


  def oldest: Option[Feature] = Option(
    try {
      track.first
    } catch {
      case _: NoSuchElementException => null
    }
  )

  def previous(feature: Feature, predicate: NumEntriesPredicate[Feature] = NumEntriesPredicate(1), ordered: Comparator[Feature] = FeatureOrder.chronological): util.NavigableSet[Feature] = {
    val features = new util.TreeSet[Feature](ordered)
    val source = headSet(feature)
    if (source.size() >= predicate.numEntries) {
      try {
        val iterator = headSet(feature).descendingIterator()
        breakable {
          while(iterator.hasNext) {
            val f = iterator.next()
            if (predicate.test(f))
              features.add(f)
            else
              break
          }
        }
      } catch {
        case _: Exception =>
      }
    }
    features
  }

  def next(feature: Feature, predicate: NumEntriesPredicate[Feature] = NumEntriesPredicate(1), ordered: Comparator[Feature] = FeatureOrder.chronological): util.NavigableSet[Feature] = {
    val features = new util.TreeSet[Feature](ordered)
    val source = tailSet(feature)
    if (source.size() > predicate.numEntries) {
      try {
        val iterator = source.iterator()
        iterator.next()
        breakable {
          while(iterator.hasNext) {
            val f = iterator.next()
            if (predicate.test(f))
              features.add(f)
            else
              break
          }
        }
      } catch {
        case _: Exception =>
      }
    }
    features
  }

  private def headSet(feature: Feature): util.NavigableSet[Feature] = {
    try {
      return new util.TreeSet[Feature](track.headSet(feature))
    } catch {
      case _: Exception =>
    }
    Collections.emptyNavigableSet()
  }

  private def tailSet(feature: Feature): util.NavigableSet[Feature] = {
    try {
      return new util.TreeSet[Feature](track.tailSet(feature))
    } catch {
      case _: Exception =>
    }
    Collections.emptyNavigableSet()
  }

  private def subSet(from: Feature, to: Feature): util.NavigableSet[Feature] = {
    try {
      return new util.TreeSet[Feature](track.subSet(from, to))
    } catch {
      case _: Exception =>
    }
    Collections.emptyNavigableSet()
  }
}

trait FeatureOrder extends Comparator[Feature] with Serializable

class ChronologicalFeatureOrder extends FeatureOrder {
  override def compare(feature1: Feature, feature2: Feature): Int = {
    if (feature1.time.before(feature2.time))
      -1
    else if (feature1.time.after(feature2.time))
      1
    else
      0
  }
}

class ReversedFeatureOrder extends FeatureOrder {
  override def compare(feature1: Feature, feature2: Feature): Int = {
    if (feature1.time.before(feature2.time))
      1
    else if (feature1.time.after(feature2.time))
      -1
    else
      0
  }
}

object FeatureOrder {
  def chronological: Comparator[Feature] = new ChronologicalFeatureOrder()
  def reversed: Comparator[Feature] = new ReversedFeatureOrder()
}
