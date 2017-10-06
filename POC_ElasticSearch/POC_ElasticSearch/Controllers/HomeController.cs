using Serilog;
using Serilog.Sinks.Elasticsearch;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace POC_ElasticSearch.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {

            ILogger logger = new LoggerConfiguration()
                .ReadFrom.AppSettings()
                .CreateLogger();

            logger.Error(new Exception("test"), "An error has occurred.");
            logger.Information("{User} has just executed {Action}.", "Superman", "an high kick");

            return null;
        }
    }
}
