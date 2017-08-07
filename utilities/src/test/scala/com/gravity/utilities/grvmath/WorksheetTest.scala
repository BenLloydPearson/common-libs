package com.gravity.utilities.grvmath

import com.gravity.utilities.BaseScalaTest
import java.net.URLDecoder

/*             )\._.,--....,'``.
 .b--.        /;   _.. \   _\  (`._ ,.
`=,-,-'~~~   `----(,_..'--(,_..'`-.;.'  */


class WorksheetTest extends BaseScalaTest {

  test("Test non zero z scores") {
    val wk = new Worksheet[String,String]()
    wk.add("A","A","col1",5.6)
    wk.add("B","B","col1",2.6)
    wk.add("C","C","col1",3.6)
    wk.add("D","D","col1",8.6)
    wk.add("E","D","col1",0.0)
    wk.add("F","D","col1",0.0)
    wk.add("G","D","col1",0.0)
    wk.add("H","D","col1",0.0)

    wk.zscoreNonZeros("col1","zscoreNonZeroes")
    wk.zscore("col1","zscoreComplete")
    wk.printTable()
  }


//  test("Quantile Normalization") {
//    val wk = new Worksheet[String,String]()
//
//    wk.add("A","A","col1", 5)
//    wk.add("B","B","col1", 2)
//    wk.add("C","C","col1", 3)
//    wk.add("D","D","col1", 4)
//
//    wk.add("A","A","col2", 4)
//    wk.add("B","B","col2", 1)
//    wk.add("C","C","col2", 4)
//    wk.add("D","D","col2", 2)
//
//    wk.add("A","A","col3", 3)
//    wk.add("B","B","col3", 4)
//    wk.add("C","C","col3", 6)
//    wk.add("D","D","col3", 8)
//
//    wk.quantileNormalize(Seq("col1","col2","col3"),"_qn","_qp")
//    wk.printTable()
//
//  }

  test("Worksheet Operations") {
    val wk = new Worksheet[String,String]()
    wk.add("mysite","mysite","views",6.0)
    wk.add("mysite2","mysite2","views",3.0)
    wk.add("mysite","orangutans",10.0)
    wk.zscore("views","views_sig")
    wk.compute("halfViews"){row=>
      row("views") / 2.0
    }
    wk.rank("views","views_rank")
    wk.mean("views","views_mean")
    wk.print()
    println("Mean of views: " + wk.summary("views_mean"))

    wk.percentileOnDistribution("views","views_pcd")

    wk.adjustedScoreAsPercentile("views","views_padj")
    wk.adjustedScoreAsPercentile("views_sig","views_sig_padj")
    wk.print()

    println("TSV")
    println(wk.toEncodedTsv)
    println("Decoded")
    println(URLDecoder.decode(wk.toEncodedTsv,"UTF-8"))

  }

  case class Cat(name:String, age:Int, weight:Double)

  test("Worksheet Example Operations 1") {

    val catList =
      Cat("Rory", 10, 12.6) ::
      Cat("Suki", 10, 10.8) ::
      Cat("Efrem", 9, 14.2) ::
      Cat("Scout", 9, 14.1) ::
      Cat("Snu", 1, 5.8) ::
      Nil

    val wk = new Worksheet[String,Cat]()

    catList.foreach{cat=>
      wk.add(key=cat.name, original=cat, score="age", value=cat.age)
      wk.add(key=cat.name, original=cat, score="weight", value=cat.weight)
    }

    println("Populate with several cats")
    wk.print()
    wk.printTable()


    println("Weight-age-ratio")
    wk.compute("weight_age_ratio"){row=>
      row("weight") / row("age")
    }

    println("Added weight_age_ratio")
    wk.printTable()

    wk.add("Rory","weight",20)
    println("Updated Rory's weight")
    wk.printTable()

    wk.compute("weight_age_ratio_x2"){row=>
      row("weight_age_ratio") * 2

    }

    wk.add("Rory","weight",40)
    println("Added weight age ratio x2")
    wk.printTable()

    println("Added weight_age_ratio rank")
    wk.rank("weight_age_ratio","weight_age_ratio_rank")
    wk.rank("age","age_rank")
    wk.printTable()

    println("Standard Deviation Units")
    wk.zscore("weight","weight_z")
    wk.rank("weight_z","weight_z_rank")
    wk.printTable()


    println("Preparing to calculate the mean")
    wk.summarize("totalRows",wk.data.size)

    wk.accumulate("totalAge"){(total, row)=>
       total + row("age")
    }

    wk.summarize("averageAge", () => { wk.summary("totalAge") / wk.summary("totalRows")})

    wk.printSummary()

    println("Distance from average age")
    wk.compute("distance_from_average_age"){row=>
      math.abs(row("age") - wk.summary("averageAge"))
    }

    wk.printTable()

    println("Pearson Age to Weight")
    wk.pearson("age","weight","ageWeight_R")

    wk.printSummary()

    println("Normalizing Rory's weight to pearson")
    wk.add("Rory","weight",12)

    wk.printSummary()

    println("Fancy Score 1")
    wk.compute("fancyScore1"){row=>
      if(row("age") > 8) row("age")
      else row("weight")
    }

    wk.printTable()

    println("Fancy Score 2")
    wk.computeAndAnnotate("fancyScore2") {row=>
      if(row("age") > 8) (row("age") -> ("Computed because the age, which is " + row("age").toString + " is below 8"))
      else{
        (row("weight") -> ("Used weight because age " + row("age").toString + " is below 8"))
      }
    }

    wk.printTable()

    wk.compute("agePlusWeight") {row=>
      val cat: Cat = row.original  //Strong type annotation to call out what row.original's tyle is
      cat.age + cat.weight
    }
  }
//  test("Spreadsheet Operations") {
//    val sp = new Spreadsheet[String,String,String]()
//
//    sp.wk("section1").add("page1_1","views",10)
//    sp.wk("section1").add("page1_2","views",6)
//    sp.wk("section2").add("page2_1","views",5)
//    sp.wk("section2").add("page2_2","views",8)
//
//    println("Hi")
//
//  }


}
