using System;
using System.Collections.Generic;
using System.Text;

namespace AmazonReviewRandom.DataTrainer
{
    public class Review
    {
        public string reviewerID { get; set; }
        public string asin { get; set; }
        public string reviewerName { get; set; }
        public List<int> helpful { get; set; }
        public string reviewText { get; set; }
        public double overall { get; set; }
        public string summary { get; set; }
        public int unixReviewTime { get; set; }
        public string reviewTime { get; set; }
    }

}
