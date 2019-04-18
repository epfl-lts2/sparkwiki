package ch.epfl.lts2.wikipedia

import java.time.LocalDate
import com.google.common.collect.DiscreteDomain
import java.time.temporal.ChronoUnit.DAYS

class DiscreteDomainLocalDate extends DiscreteDomain[LocalDate] {
  override def next(value: LocalDate): LocalDate = value.plusDays(1)

  override def previous(value: LocalDate): LocalDate = value.minusDays(1)

  override def distance(start: LocalDate, end: LocalDate): Long = DAYS.between(start, end)
}

