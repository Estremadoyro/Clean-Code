//Clean Code
object Repository {
    def getUser(userName: String): String = userName.split("@")(0)
    def loginUser(userName: String, userPassword: String): Boolean = {
        userName == "awainer" && userPassword == "123"
    }
}
//Clean Code
class User(val userName: String, val userPassword: String) {
    require(userName != null, "User name must not be empty")
    require(userPassword != null && userPassword != "", 
            "User password must not be null")
}
//Clean Code
object Login {
    def checkLogin(user: User): Boolean = {
        val userParsed = Repository.getUser(user.userName)
        val userVerified = Repository.loginUser(userParsed, user.userPassword)
    
        if (userVerified) {
            println("Login successful"); return true 
        }
        println("Invalid username or password"); false
    }
}
