package dml

import opt.{QueryInstruction, Transformation}

class transforms {

}

class Identity(input : QueryInstruction) extends Transformation(input) {
  override def transform: QueryInstruction = {
    input
  }
}