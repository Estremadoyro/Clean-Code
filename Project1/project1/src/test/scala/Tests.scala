import collection.mutable.Stack;
import org.scalatest.flatspec;
import org.scalatest.matchers;
import flatspec.AnyFlatSpec;
import matchers.should;

class EjercicioTests extends AnyFlatSpec with should.Matchers {
  "Ejercicio1" should "Login user" in {
    val user3 = new User("awainer@demo.com", "123");
    Login.checkLogin(user3) shouldBe true;
  }
  "Ejercicio1" should "Not login user" in {
    val user1 = new User("awainer@demo.com", "1234");
    val user2 = new User("", "123"); 
    Login.checkLogin(user1) shouldBe false;
    Login.checkLogin(user2) shouldBe false;
  }
}
