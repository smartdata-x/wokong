package request

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by C.J.YOU on 2016/9/21.
  */
class TokenTest extends FlatSpec with Matchers {

  it should "get none empty token " in  {

    val token = Token.token()

    token should not be ""

  }

}
