package experiment

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import dml.Join

import scala.collection.mutable.{ArrayBuffer, Set}
import opt.{ConstraintStub, QueryInstruction, RelationStub}

object Sandbox {
  def main(args: Array[String]) = {
    val r1 = new RelationStub("a", Set(Array("abcd", "efgh"), Array("ijkl", "mnop")))
    val r2 = new RelationStub("a", Set(Array("qrst", "uvwx"), Array("yzab", "cdef")))
    val relations = new ArrayBuffer[Either[RelationStub, QueryInstruction]]()
    relations+=Left(r1)
    relations+=Left(r2)
    val find = new Join(relations, new ArrayBuffer[ConstraintStub]())
    val find2 = find.deepClone
    find2.relations(0).left.get.relationName = "change the relation name"
    println(find.relations(0).left.get.relationName)
    println(find2.relations(0).left.get.relationName)
  }
}
