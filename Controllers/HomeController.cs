using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using AmazonReviewRandom.Models;
using Microsoft.Extensions.Configuration;
using AmazonReviewRandom.DataTrainer;

namespace AmazonReviewRandom.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private static CancellationTokenSource token;
        private IConfiguration _config;
        public HomeController(ILogger<HomeController> logger,IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        public IActionResult StartTraining()
        {
            if (token == null)
            {
                token = new CancellationTokenSource();
                var fileName = _config["GzipFileName"];
                var buggerSize = int.Parse(_config["BufferSize"]);
                var connectionString = _config["ConnectionString"];
                Task.Run(() =>
                {
                    new DataTrainer.DataTrainer(fileName, buggerSize, connectionString).TrainData(token);
                    token = null;
                });
                return Json("Training is started successfully.");
            }
            else
            {
                return Json("Training is already running.");
            }

        }

        public IActionResult StopTraining()
        {
            if (token != null && !token.IsCancellationRequested)
            {
                token.Cancel();
                return Json("Training is stopped successfully");
            }
           
            return Json("No training is running.");
        }

        public IActionResult ClearData()
        {
            var connectionString = _config["ConnectionString"];
            Task.Run(() =>
            {
                new DataTrainer.DataTrainer(connectionString).ClearData();
            });
            return Json(true);
        }
        public IActionResult GenerateReview()
        {
            Random random = new Random();

            var reviewersNames = _config["ReviewersName"];
            var reviewersNamesLst = reviewersNames.Split(',').ToList();
            var reviewerName = reviewersNamesLst[random.Next(1, 49)];

            var connectionString = _config["ConnectionString"];
            var keySize = int.Parse(_config["KeySize"]);
            var outputSize = int.Parse(_config["OutputSize"]);
            var reviewText = new DataTrainer.DataTrainer(connectionString).GenerateReview(keySize, outputSize);
            var review = new Review();
            review.reviewText = reviewText;
            review.reviewerName = reviewerName;
            review.overall = random.Next(1, 6);

            return Json(review);
        }
    }
}
