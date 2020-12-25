package cn.tongyongtao.Day12

import scala.beans.BeanProperty

class People {
  @BeanProperty var name: String = _
  @BeanProperty var age: Int = _
  var salary: Double = _

  def setSalary(salary: Double): Unit = {
    this.salary = salary
  }

  override def toString = s"People($name, $age, $salary)"


  implicit  def order(p:People): Ordering[People] =
    new Ordering[People] {
      override def compare(x: People, y: People): Int = {
        if (x.age == y.age) {
          -java.lang.Double.compare(x.salary, y.salary)
        }
        else {
          x.name.compareTo(y.name)
        }
      }
    }

}
