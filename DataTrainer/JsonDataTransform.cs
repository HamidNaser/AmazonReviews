
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;

namespace AmazonReviewRandom.DataTrainer
{
    public class JsonDataTransform
    {
        /// <summary>
        /// Last line might be cut off when we read a set of data to the buffer. 
        /// We will concatinate it with the first line from the next read.
        /// </summary>
        private string LastIncompleteLine = string.Empty;

        /// <summary>
        /// Transform json data to Review objects.
        /// </summary>
        /// <param name="rawData"></param>
        /// <returns></returns>
        public List<Review> TransformJsonData(string rawData)
        {
            var reviews = new List<Review>();

            var arrLines = rawData.Split("\n\r".ToCharArray(), StringSplitOptions.RemoveEmptyEntries);

            for (var index = 0; index < arrLines.Count(); index++)
            {
                try
                {
                    var review = JsonSerializer.Deserialize<Review>(arrLines[index]);

                    if (review != null && !string.IsNullOrEmpty(review.reviewText))
                    {
                        reviews.Add(review);
                    }
                }
                catch (Exception exp)
                {
                    if (index == arrLines.Count() - 1)
                    {
                        LastIncompleteLine = arrLines[index];
                    }
                    else if (index == 0)
                    {
                        var review = RepairAndTransformDataToObject(string.Format("{0}{1}", LastIncompleteLine, arrLines[index]));
                        if (review != null && !string.IsNullOrEmpty(review.reviewText))
                        {
                            reviews.Add(review);
                        }
                        LastIncompleteLine = string.Empty;
                    }

                }
            }

            return reviews;
        }

        private Review RepairAndTransformDataToObject(string rawData)
        {
            try
            {
                var review = JsonSerializer.Deserialize<Review>(rawData);

                if (review != null && !string.IsNullOrEmpty(review.reviewText))
                {
                    return review;
                }
            }
            catch (Exception ex)
            {

            }

            return null;
        }
    }
}
