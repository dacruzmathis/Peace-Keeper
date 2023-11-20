package Service

object Utils {

  def resultToString[A](result: List[A] ): String = result match {
    case Nil => ""
    case t :: q => t + "\n" + resultToString(q)
  }

  def resultToString[A](result: Iterator[A]): String = result match {
    case x if !x.hasNext => ""
    case x => x.next() + "\n" + resultToString(x)
  }
}
