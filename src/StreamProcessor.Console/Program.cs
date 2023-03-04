// See https://aka.ms/new-console-template for more information
using Microsoft.Extensions.Logging;
using System.Buffers;
using System.ComponentModel;
using System.ComponentModel.Design;
using System.Diagnostics.Metrics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;
using SlugEnt.StreamProcessor;
using System.Xml.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Serilog;
using Serilog.Events;
using Spectre.Console;
using StreamProcessor.Console;
using StreamProcessor.Console.SampleA;
using StreamProcessor.Console.SampleB;
using StreamProcessor.ConsoleScr;
using StreamProcessor.ConsoleScr.SampleB;
using StreamProcessor.ConsoleScr.SampleC;
using Serilog.Core;
using System;


public class Program
{
    static async Task Main(string[] args)
    {
        Serilog.ILogger Logger;
        Log.Logger = new LoggerConfiguration()
#if DEBUG
            .MinimumLevel.Verbose()
            .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
#else
						 .MinimumLevel.Information()
			             .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
#endif
            
            .WriteTo.Console()
            .WriteTo.Debug()
            .Enrich.FromLogContext()
            .CreateLogger();

        Log.Debug("Starting " + Assembly.GetEntryAssembly().FullName);

        Log.Information("Main Info Log");
        Log.Debug("Main Debug log");

        try
        {
            var host = CreateHostBuilder(args).Build();
            MainMenu mainMenu = host.Services.GetService<MainMenu>();

            ILoggerFactory loggerFactory = host.Services.GetService<ILoggerFactory>();
            ILogger<Program> plogger = loggerFactory.CreateLogger<Program>();
            plogger.LogInformation("plogger info");
            plogger.LogDebug("Plogger debug");

            using (plogger.BeginScope("program scope"))
            {
                plogger.LogInformation("some info");
                plogger.LogDebug("its broke");
                plogger.LogWarning("warning");
                plogger.LogCritical("critical");
                plogger.LogTrace("traced");
            }

            mainMenu.Display();
            host.StartAsync();
            await host.WaitForShutdownAsync();
            AnsiConsole.WriteLine("Exiting");
        }
        catch (Exception ex)
        {
            AnsiConsole.WriteLine("Error");

        }
        AnsiConsole.WriteLine("Bye");
    }


    /// <summary>
    /// Creates the Host
    /// </summary>
    /// <param name="args"></param>
    /// <returns></returns>
    static IHostBuilder CreateHostBuilder(string[] args) =>
        Host.CreateDefaultBuilder(args)
            .ConfigureServices((_, services) =>
            {
                services.AddTransient<SampleBApp,SampleBApp>();
                services.AddTransient<MainMenu>();
                services.AddTransient<IMqStreamConsumer, MqStreamConsumer>();
                services.AddTransient<IMqStreamProducer, MqStreamProducer>();
                services.AddTransient<IMQStreamEngine,MQStreamEngine>();
                services.AddTransient<ISampleB_Consumer,SampleB_Consumer>();
                services.AddTransient<ISampleB_Producer, SampleB_Producer>();
                services.AddTransient<Sample_Z>();
                services.AddTransient<SampleCApp>();

            })
            .ConfigureLogging((_, logging) =>
            {
                logging.ClearProviders();
                logging.AddSerilog();
                logging.AddDebug();
                logging.AddConsole();
                //logging.AddSimpleConsole(options => options.IncludeScopes = true);
                //logging.AddEventLog();
            });

}


