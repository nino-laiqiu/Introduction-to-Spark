package cn.tongyongtao.Day12


import scala.beans.BeanProperty

class Users extends Serializable with Ordered[Users] {
  @BeanProperty var name: String = _
  @BeanProperty var age: Int = _
  @BeanProperty var salary: Double = _

  override def toString = s"User($name, $age, $salary)"

  override def compare(that: Users): Int = {
        if (this.name == that.name){
           java.lang.Double.compare(this.salary,that.salary)
        }
     else {
           -this.age -that.age
        }
  }
/*  if (x.age < y.age) {
    java.lang.Double.compare(x.salary, y.salary)
  }
  else {
    x.name.compareTo(y.name)
  }*/
}
