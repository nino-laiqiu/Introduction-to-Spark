package cn.tongyongtao.Day12

case class Teacher(val name: String, val age: Int, val salary: Double) extends Ordering[Teacher] {
  override def compare(x: Teacher, y: Teacher): Int = {
    if (x.name > y.name) {
      java.lang.Double.compare(x.salary, y.salary)
    }
    else {
      x.name.compareTo(y.name)
    }
  }
}
