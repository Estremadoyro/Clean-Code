import collection.mutable.Stack
import org.scalatest._
import flatspec._
import matchers._

import Login.User
import Login.Login

class Ejercicio1 extends AnyFlatSpec with should.Matchers {
  "Login" should "Login a user" in {
    val user3 = new User("awainer@demo.com", "123")
    val test1 = Login.checkLogin(user3)
    test1 shouldBe true
  }
}
