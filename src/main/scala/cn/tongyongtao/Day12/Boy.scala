package cn.tongyongtao.Day12

class Boy(val name:String,val age :Int,val salary:Double) extends Comparable[Boy] with Serializable {
  override def compareTo(that: Boy): Int = {
         if (this.name != that.name ){
                this.age - that.age
         }
         else {
                - java.lang.Double.compare(this.salary,that.salary)
         }
  }
  override def toString = s"Boy($name, $age, $salary)"
}

