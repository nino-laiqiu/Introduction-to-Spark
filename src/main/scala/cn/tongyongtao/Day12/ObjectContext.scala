package cn.tongyongtao.Day12

object ObjectContext {

  implicit object order extends Ordering[Employee] {
    override def compare(x: Employee, y: Employee): Int = {
      -java.lang.Double.compare(x.salary, y.salary)
    }
  }
}
