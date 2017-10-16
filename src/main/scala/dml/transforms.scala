package dml

import opt.{QueryInstruction, Transformation}

class transforms {

}

class IdentityTransform extends Transformation {
  override def transform(input : QueryInstruction): QueryInstruction = {
    input
  }
}


class OtherTransform extends Transformation {
  override def transform(input : QueryInstruction): QueryInstruction = {
    input
  }
}
