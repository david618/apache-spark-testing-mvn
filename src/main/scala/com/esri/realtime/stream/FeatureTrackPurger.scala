package com.esri.realtime.stream

trait FeatureTrackPurger extends Serializable {
  def purge(track: FeatureTrack): Unit
}

case class MaxFeaturesPerTrackPurger(maxFeatures: Int) extends FeatureTrackPurger {
  override def purge(track: FeatureTrack): Unit = {
    if (track.size > maxFeatures)
      track.remove(track.oldest.get)
  }
}
