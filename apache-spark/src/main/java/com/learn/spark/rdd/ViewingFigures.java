package com.learn.spark.rdd;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.runtime.StringFormat;

/**
 * This class is used in the chapter late in the course where we analyse viewing figures.
 * You can ignore until then.
 */
public class ViewingFigures {

	@SuppressWarnings("resource")
	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\programs\\hadoop");
		Logger.getLogger("org.apache").setLevel(Level.WARN);

		SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// Use true to use hardcoded data identical to that in the PDF guide.
		boolean testMode = false;
		
		JavaPairRDD<Integer, Integer> viewData = setUpViewDataRdd(sc, testMode);
		JavaPairRDD<Integer, Integer> chapterData = setUpChapterDataRdd(sc, testMode);
		JavaPairRDD<Integer, String> titlesData = setUpTitlesDataRdd(sc, testMode);

		//countChaptersOfCourses(chapterData, titlesData);

		JavaPairRDD<Integer, Integer> courseTotalChapters = chapterData
				.mapToPair(chapterCourse -> new Tuple2<>(chapterCourse._2, 1))
				.reduceByKey((count, anotherCount) -> count + anotherCount);

		//courseTotalChapters.foreach(courseTotalChapter -> System.out.println(String.format("Course[%d] has total %d chapters", courseTotalChapter._1, courseTotalChapter._2)));

		JavaPairRDD<Integer, Integer> courseViewedCount = viewData
				.distinct()
				.mapToPair(userChapter -> new Tuple2<>(userChapter._2, userChapter._1))
				.join(chapterData)
				.mapToPair(
						new PairFunction<Tuple2<Integer, Tuple2<Integer, Integer>>, Tuple2<Integer, Integer>, Integer>() {
							@Override
							public Tuple2<Tuple2<Integer, Integer>, Integer> call(
									Tuple2<Integer, Tuple2<Integer, Integer>> chapterUserCourse) throws Exception {
								Tuple2<Integer, Integer> userCourse = chapterUserCourse._2;
								Tuple2<Integer, Integer> courseUser = new Tuple2<>(userCourse._2, userCourse._1);
								return new Tuple2<>(courseUser, 1);
							}
						})
				.reduceByKey((c1, c2) -> c1 + c2)
				.mapToPair(
						new PairFunction<Tuple2<Tuple2<Integer, Integer>, Integer>, Integer, Integer>() {
							@Override
							public Tuple2<Integer, Integer> call(
									Tuple2<Tuple2<Integer, Integer>, Integer> courseUserViewedCount) throws Exception {
								Tuple2<Integer, Integer> courseUser = courseUserViewedCount._1;
								Integer viewedCount = courseUserViewedCount._2;
								return new Tuple2<>(courseUser._1, viewedCount);
							}
						});

		//courseViewedCount.foreach(courseUserViewed -> System.out.println(String.format("For course[%d], %d chapters viewed", courseUserViewed._1, courseUserViewed._2)));

		JavaPairRDD<Integer, Tuple2<Integer, Integer>> courseTotalViewedCount = courseTotalChapters
				.join(courseViewedCount);

		JavaPairRDD<Integer, Integer> courseScore = courseTotalViewedCount.mapValues(new Function<Tuple2<Integer, Integer>, Double>() {
			@Override
			public Double call(Tuple2<Integer, Integer> totalViews) throws Exception {
				Integer total = totalViews._1;
				Integer actualViews = totalViews._2;
				return (actualViews * 1.0) / total;
			}
		}).mapValues(percentage -> {
			if (percentage > 0.9) return 10;
			else if (percentage > 0.5) return 4;
			else if (percentage > 0.25) return 2;
			else return 0;
		});

		JavaPairRDD<Integer, Tuple2<Integer, String>> courseScoreName = courseScore
				.reduceByKey((score1, score2) -> score1 + score2)
				.join(titlesData);

		int numPartitions = courseScoreName.getNumPartitions();
		System.out.println("Number of partitions " + numPartitions);

		courseScoreName
				.mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Integer, String>>, Integer, Tuple2<Integer, String>>() {
					@Override
					public Tuple2<Integer, Tuple2<Integer, String>> call(
							Tuple2<Integer, Tuple2<Integer, String>> scoreCourseTitle) throws Exception {
						Integer courseId = scoreCourseTitle._1;
						Tuple2<Integer, String> scoreName = scoreCourseTitle._2;
						return new Tuple2<>(scoreName._1, new Tuple2<>(courseId, scoreName._2));
					}
				})
				.sortByKey(false)
				.coalesce(1)
				.foreach(scoreCourseTitle -> System.out.println(String.format("Course '%s'[%d] has score %d", scoreCourseTitle._2._2, scoreCourseTitle._2._1, scoreCourseTitle._1)));

		sc.close();
	}

	private static void countChaptersOfCourses(JavaPairRDD<Integer, Integer> chapterData,
			JavaPairRDD<Integer, String> titlesData) {
		chapterData.mapToPair(chapterCourse -> new Tuple2<>(chapterCourse._2, 1))
				.reduceByKey((Function2<Integer, Integer, Integer>) (count1, count2) -> count1 + count2)
				.join(titlesData)
				.foreach(course -> System.out.println(String.format("Course '%s'[%d] has %d chapters", course._2._2, course._1, course._2._1)));
	}

	private static JavaPairRDD<Integer, String> setUpTitlesDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode) {
			// (chapterId, title)
			List<Tuple2<Integer, String>> rawTitles = new ArrayList<>();
			rawTitles.add(new Tuple2<>(1, "How to find a better job"));
			rawTitles.add(new Tuple2<>(2, "Work faster harder smarter until you drop"));
			rawTitles.add(new Tuple2<>(3, "Content Creation is a Mug's Game"));
			return sc.parallelizePairs(rawTitles);
		}
		return sc.textFile("big-tada/apache-spark/src/main/resources/viewing figures/titles.csv")
				                                    .mapToPair(commaSeparatedLine -> {
														String[] cols = commaSeparatedLine.split(",");
														return new Tuple2<Integer, String>(new Integer(cols[0]),cols[1]);
				                                    });
	}

	private static JavaPairRDD<Integer, Integer> setUpChapterDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode) {
			// (chapterId, (courseId, courseTitle))
			List<Tuple2<Integer, Integer>> rawChapterData = new ArrayList<>();
			rawChapterData.add(new Tuple2<>(96,  1));
			rawChapterData.add(new Tuple2<>(97,  1));
			rawChapterData.add(new Tuple2<>(98,  1));

			rawChapterData.add(new Tuple2<>(99,  2));

			rawChapterData.add(new Tuple2<>(100, 3));
			rawChapterData.add(new Tuple2<>(101, 3));
			rawChapterData.add(new Tuple2<>(102, 3));
			rawChapterData.add(new Tuple2<>(103, 3));
			rawChapterData.add(new Tuple2<>(104, 3));
			rawChapterData.add(new Tuple2<>(105, 3));
			rawChapterData.add(new Tuple2<>(106, 3));
			rawChapterData.add(new Tuple2<>(107, 3));
			rawChapterData.add(new Tuple2<>(108, 3));
			rawChapterData.add(new Tuple2<>(109, 3));
			return sc.parallelizePairs(rawChapterData);
		}

		return sc.textFile("big-tada/apache-spark/src/main/resources/viewing figures/chapters.csv")
													  .mapToPair(commaSeparatedLine -> {
															String[] cols = commaSeparatedLine.split(",");
															return new Tuple2<Integer, Integer>(new Integer(cols[0]), new Integer(cols[1]));
													  	});
	}

	private static JavaPairRDD<Integer, Integer> setUpViewDataRdd(JavaSparkContext sc, boolean testMode) {
		
		if (testMode) {
			// Chapter views - (userId, chapterId)
			List<Tuple2<Integer, Integer>> rawViewData = new ArrayList<>();
			rawViewData.add(new Tuple2<>(14, 96));
			rawViewData.add(new Tuple2<>(14, 97));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));
			rawViewData.add(new Tuple2<>(13, 96));

			rawViewData.add(new Tuple2<>(14, 99));

			rawViewData.add(new Tuple2<>(13, 100));
			return  sc.parallelizePairs(rawViewData);
		}

		//Support for -* to read multiple files matching
		return sc.textFile("big-tada/apache-spark/src/main/resources/viewing figures/views-*.csv")
				     .mapToPair(commaSeparatedLine -> {
				    	 String[] columns = commaSeparatedLine.split(",");
				    	 return new Tuple2<Integer, Integer>(new Integer(columns[0]), new Integer(columns[1]));
				     });
	}
}
