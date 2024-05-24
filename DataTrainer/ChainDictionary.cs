using System;
using System.Collections.Generic;
using System.Text;

namespace AmazonReviewRandom.DataTrainer
{
    internal static class ChainDictionary
    {
        public static Dictionary<string,List<string>> GetMarkOvChainDictionary(this MarkovChain markovChain, JsonDataTransform jsonDataTransform,byte[] buffer)
        {
            markovChain = new MarkovChain();
            string rawData = Encoding.UTF8.GetString(buffer, 0, buffer.Length);
            var reviews = jsonDataTransform.TransformJsonData(rawData).GetReviewsAsString();           
            //File.WriteAllText(@"C:\Hamid\Code\alice_oz_a.txt", stringBuilder.ToString());
            return markovChain.Chain(reviews, 3, 200);
        }
        private static string GetReviewsAsString(this List<Review> reviews)
        {
            var stringBuilder = new StringBuilder();
            //reviews.ForEach(x =>
            //{
            //    stringBuilder.Append(". " + x.reviewText);
            //});
            reviews.ForEach(x => stringBuilder.AppendMe(x.reviewText));
            return stringBuilder.ToString();
        }
        private static void AppendMe(this StringBuilder text,string toAppend)
        {
            text.Append(". " + toAppend);
        }
    }
}
