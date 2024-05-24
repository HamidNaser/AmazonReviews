using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace AmazonReviewRandom.DataTrainer
{
    public class MarkovChain
    {
        private string Join(string a, string b)
        {
            return a + " " + b;
        }

        public Dictionary<string, List<string>> Chain(string line, int keySize, int outputSize)
        {
            var WordsDict = new Dictionary<string, List<string>>();
            var words = line.Split();
            if (outputSize < keySize)//|| words.Length < outputSize)
            {
                throw new ArgumentException("Output size is out of range");
            }

            for (int i = 0; i < words.Length - keySize; i++)
            {
                var key = words.Skip(i).Take(keySize).Aggregate(Join);
                string value;
                if (i + keySize < words.Length)
                {
                    value = words[i + keySize];
                }
                else
                {
                    value = "";
                }

                if (WordsDict.ContainsKey(key))
                {
                    WordsDict[key].Add(value.Trim());
                }
                else
                {
                    WordsDict.Add(key, new List<string>() { value.Trim() });
                }
            }

            return WordsDict;
        }
    }
}
