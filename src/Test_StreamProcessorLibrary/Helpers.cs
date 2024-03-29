﻿using System.Net;
using AutoFixture;
using AutoFixture.AutoMoq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using RabbitMQ.Stream.Client;
using RabbitMQ.Stream.Client.Reliable;

namespace Test_StreamProcessorLibrary;

public static class Helpers
{
    /// <summary>
    /// Sends the given number of messages to the producerTst. Returns the last message #
    /// </summary>
    /// <param name="producerTst"></param>
    /// <param name="quantity"></param>
    /// <param name="startingMsgNumber"></param>
    internal static int SendTestMessages(MqTesterProducer producerTst, int quantity, int startingMsgNumber)
    {
        int i;
        int x = startingMsgNumber + quantity;


        for (i = startingMsgNumber; i < x; i++)
        {
            producerTst.SendMessageAsync("Msq #" + i);
        }

        return i;
    }



    /// <summary>
    /// Builds the StreamSystemConfig
    /// </summary>
    /// <returns></returns>
    internal static StreamSystemConfig GetStreamConfig()
    {
        IPAddress  address = IPAddress.Loopback;
        IPEndPoint a       = IPEndPoint.Parse("127.0.0.1:5552");

        StreamSystemConfig config = new StreamSystemConfig
        {
            UserName = "testUser", Password = "TESTUSER", VirtualHost = "Test", Endpoints = new List<EndPoint> { a },
        };
        return config;
    }



    internal static (int sent, int confirmed) SendAndConfirmTestMessages(MqTesterProducer producerTst, int quantity, int startingMsgNumber,
                                                                         ConfirmationStatus status)
    {
        int count     = SendTestMessages(producerTst, quantity, startingMsgNumber);
        int confirmed = producerTst.TST_ReturnProducerConfirmations(quantity, status);
        return (count, confirmed);
    }


    public static MqTesterProducer SetupProducer(string streamName = "produce", string appName = "a")
    {
        Fixture fixture = new Fixture();
        fixture.Customize(new AutoMoqCustomization());


        ServiceCollection services = new ServiceCollection();
        services.AddLogging();
        ServiceProvider  serviceProvider = services.BuildServiceProvider();
        MqTesterProducer producerTst     = new MqTesterProducer(new NullLogger<MqTesterProducer>(), serviceProvider);

        StreamSystemConfig config = GetStreamConfig();
        producerTst.Initialize(streamName, appName, config);
        return producerTst;
    }


    public static MqTesterConsumer SetupConsumer(string streamName = "produce", string appName = "a")
    {
        Fixture fixture = new Fixture();
        fixture.Customize(new AutoMoqCustomization());

        ServiceCollection services = new ServiceCollection();
        services.AddLogging();
        ServiceProvider  serviceProvider = services.BuildServiceProvider();
        MqTesterConsumer consumerTst     = new MqTesterConsumer(new NullLogger<MqTesterConsumer>(), serviceProvider);

        StreamSystemConfig config = GetStreamConfig();
        consumerTst.Initialize(streamName, appName, config);
        return consumerTst;
    }
}