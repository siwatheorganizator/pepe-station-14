using System.Collections.ObjectModel;
using System.Threading.Tasks;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text.Json;
using System.Threading;
using Content.Shared.CCVar;
using NetCord;
using NetCord.Gateway;
using NetCord.Rest;
using Robust.Shared.Configuration;
using Robust.Shared.Utility;

namespace Content.Server.Discord.DiscordLink;

/// <summary>
/// Represents the arguments for the <see cref="DiscordLink.OnCommandReceived"/> event.
/// </summary>
public sealed class CommandReceivedEventArgs
{
    /// <summary>
    /// The command that was received. This is the first word in the message, after the bot prefix.
    /// </summary>
    public string Command { get; init; } = string.Empty;

    /// <summary>
    /// The raw arguments to the command. This is everything after the command
    /// </summary>
    public string RawArguments { get; init; } = string.Empty;

    /// <summary>
    /// A list of arguments to the command.
    /// This uses <see cref="CommandParsing.ParseArguments"/> mostly for maintainability.
    /// </summary>
    public List<string> Arguments { get; init; } = [];

    /// <summary>
    /// Information about the message that the command was received from. This includes the message content, author, etc.
    /// Use this to reply to the message, delete it, etc.
    /// </summary>
    public Message Message { get; init; } = default!;
}

public sealed record DiscordGuildMemberRoles(bool IsGuildMember, IReadOnlySet<ulong> RoleIds);

/// <summary>
/// Handles the connection to Discord and provides methods to interact with it.
/// </summary>
public sealed class DiscordLink : IPostInjectInit
{
    private static readonly HttpClient Http = new();

    [Dependency] private readonly ILogManager _logManager = default!;
    [Dependency] private readonly IConfigurationManager _configuration = default!;

    /// <summary>
    ///    The Discord client. This is null if the bot is not connected.
    /// </summary>
    /// <remarks>
    ///     This should not be used directly outside of DiscordLink. So please do not make it public. Use the methods in this class instead.
    /// </remarks>
    private GatewayClient? _client;
    private CancellationTokenSource? _connectionCancel;
    private Task? _connectionTask;
    private ISawmill _sawmill = default!;
    private ISawmill _sawmillLog = default!;

    private ulong _guildId;
    private string _botToken = string.Empty;

    public string BotPrefix = default!;
    /// <summary>
    /// If the bot is currently connected to Discord.
    /// </summary>
    public bool IsConnected => _client != null;

    #region Events

    /// <summary>
    ///     Event that is raised when a command is received from Discord.
    /// </summary>
    public event Action<CommandReceivedEventArgs>? OnCommandReceived;
    /// <summary>
    ///     Event that is raised when a message is received from Discord. This is raised for every message, including commands.
    /// </summary>
    public event Action<Message>? OnMessageReceived;

    // TODO: consider implementing this in a way where we can unregister it in a similar way
    public void RegisterCommandCallback(Action<CommandReceivedEventArgs> callback, string command)
    {
        OnCommandReceived += args =>
        {
            if (args.Command == command)
                callback(args);
        };
    }

    #endregion

    public void Initialize()
    {
        _configuration.OnValueChanged(CCVars.DiscordGuildId, OnGuildIdChanged, true);
        _configuration.OnValueChanged(CCVars.DiscordPrefix, OnPrefixChanged, true);

        if (_configuration.GetCVar(CCVars.DiscordToken) is not { } token || token == string.Empty)
        {
            _sawmill.Info("No Discord token specified, not connecting.");
            return;
        }

        _botToken = token;

        // If the Guild ID is empty OR the prefix is empty, we don't want to connect to Discord.
        if (_guildId == 0 || BotPrefix == string.Empty)
        {
            // This is a warning, not info, because it's a configuration error.
            // It is valid to not have a Discord token set which is why the above check is an info.
            // But if you have a token set, you should also have a guild ID and prefix set.
            _sawmill.Warning("No Discord guild ID or prefix specified, not connecting.");
            return;
        }

        // Since you cannot change the token while the server is running / the DiscordLink is initialized,
        // we can just set the token without updating it every time the cvar changes.
        _connectionCancel = new CancellationTokenSource();
        _connectionTask = Task.Run(() => ConnectLoop(token, _connectionCancel.Token));
    }

    public async Task Shutdown()
    {
        if (_connectionCancel != null)
        {
            await _connectionCancel.CancelAsync();
            _connectionCancel.Dispose();
            _connectionCancel = null;
        }

        if (_connectionTask != null)
        {
            try
            {
                await _connectionTask;
            }
            catch (OperationCanceledException)
            {
            }

            _connectionTask = null;
        }

        var client = _client;
        _client = null;
        if (client != null)
        {
            _sawmill.Info("Disconnecting from Discord.");

            client.MessageCreate -= OnCommandReceivedInternal;
            client.MessageCreate -= OnMessageReceivedInternal;

            await client.CloseAsync();
            client.Dispose();
        }

        _configuration.UnsubValueChanged(CCVars.DiscordGuildId, OnGuildIdChanged);
        _configuration.UnsubValueChanged(CCVars.DiscordPrefix, OnPrefixChanged);
    }

    private async Task ConnectLoop(string token, CancellationToken cancel)
    {
        var retryDelay = TimeSpan.FromSeconds(10);

        while (!cancel.IsCancellationRequested)
        {
            var client = CreateClient(token);
            _client = client;
            var connected = false;

            try
            {
                using var attemptCancel = CancellationTokenSource.CreateLinkedTokenSource(cancel);
                attemptCancel.CancelAfter(TimeSpan.FromSeconds(30));

                await client.StartAsync(cancellationToken: attemptCancel.Token);
                connected = true;
                _sawmill.Info("Connected to Discord.");
                return;
            }
            catch (OperationCanceledException) when (cancel.IsCancellationRequested)
            {
                return;
            }
            catch (OperationCanceledException)
            {
                _sawmill.Warning("Discord connection attempt timed out. Retrying in {Delay} seconds.", retryDelay.TotalSeconds);
            }
            catch (Exception e)
            {
                _sawmill.Error("Failed to connect to Discord: {Exception}", e);
            }
            finally
            {
                if (!connected)
                {
                    if (ReferenceEquals(_client, client))
                        _client = null;

                    client.MessageCreate -= OnCommandReceivedInternal;
                    client.MessageCreate -= OnMessageReceivedInternal;
                    client.Dispose();
                }
            }

            try
            {
                await Task.Delay(retryDelay, cancel);
            }
            catch (OperationCanceledException)
            {
                return;
            }
        }
    }

    private GatewayClient CreateClient(string token)
    {
        var client = new GatewayClient(new BotToken(token), new GatewayClientConfiguration()
        {
            Intents = GatewayIntents.Guilds
                             | GatewayIntents.GuildUsers
                             | GatewayIntents.GuildMessages
                             | GatewayIntents.MessageContent
                             | GatewayIntents.DirectMessages,
            Logger = new DiscordSawmillLogger(_sawmillLog),
        });

        client.MessageCreate += OnCommandReceivedInternal;
        client.MessageCreate += OnMessageReceivedInternal;
        client.Ready += _ =>
        {
            _sawmill.Info("Discord client ready.");
            return default;
        };

        return client;
    }

    void IPostInjectInit.PostInject()
    {
        _sawmill = _logManager.GetSawmill("discord.link");
        _sawmillLog = _logManager.GetSawmill("discord.link.log");
    }

    private void OnGuildIdChanged(string guildId)
    {
        _guildId = ulong.TryParse(guildId, out var id) ? id : 0;
    }

    private void OnPrefixChanged(string prefix)
    {
        BotPrefix = prefix;
    }

    private ValueTask OnCommandReceivedInternal(Message message)
    {
        var content = message.Content;
        // If the message doesn't start with the bot prefix, ignore it.
        if (!content.StartsWith(BotPrefix))
            return ValueTask.CompletedTask;

        // Split the message into the command and the arguments.
        var trimmedInput = content[BotPrefix.Length..].Trim();
        var firstSpaceIndex = trimmedInput.IndexOf(' ');

        string command, rawArguments;

        if (firstSpaceIndex == -1)
        {
            command = trimmedInput;
            rawArguments = string.Empty;
        }
        else
        {
            command = trimmedInput[..firstSpaceIndex];
            rawArguments = trimmedInput[(firstSpaceIndex + 1)..].Trim();
        }

        var argumentList = new List<string>();
        CommandParsing.ParseArguments(rawArguments, argumentList);

        // Raise the event!
        OnCommandReceived?.Invoke(new CommandReceivedEventArgs
        {
            Command = command,
            Arguments = argumentList,
            RawArguments = rawArguments,
            Message = message,
        });
        return ValueTask.CompletedTask;
    }

    private ValueTask OnMessageReceivedInternal(Message message)
    {
        OnMessageReceived?.Invoke(message);
        return ValueTask.CompletedTask;
    }

    #region Proxy methods

    /// <summary>
    /// Sends a message to a Discord channel with the specified ID. Without any mentions.
    /// </summary>
    public async Task SendMessageAsync(ulong channelId, string message)
    {
        if (_client == null)
        {
            return;
        }

        var channel = await _client.Rest.GetChannelAsync(channelId) as TextChannel;
        if (channel == null)
        {
            _sawmill.Error("Tried to send a message to Discord but the channel {Channel} was not found.", channel);
            return;
        }

        await channel.SendMessageAsync(new MessageProperties()
        {
            AllowedMentions = AllowedMentionsProperties.None,
            Content = message,
        });
    }

    public async Task<DiscordGuildMemberRoles?> GetGuildMemberRolesAsync(ulong discordUserId, CancellationToken cancel = default)
    {
        if (_guildId == 0 || string.IsNullOrWhiteSpace(_botToken))
            return null;

        using var request = new HttpRequestMessage(
            HttpMethod.Get,
            $"https://discord.com/api/v10/guilds/{_guildId}/members/{discordUserId}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bot", _botToken);
        request.Headers.UserAgent.ParseAdd("SpaceStation14-DiscordRankSync/1.0");

        try
        {
            using var response = await Http.SendAsync(request, cancel);

            if (response.StatusCode == HttpStatusCode.NotFound)
                return new DiscordGuildMemberRoles(false, new HashSet<ulong>());

            if (!response.IsSuccessStatusCode)
            {
                _sawmill.Error(
                    "Discord guild member lookup for user {DiscordUserId} failed with HTTP {StatusCode}.",
                    discordUserId,
                    response.StatusCode);
                return null;
            }

            await using var stream = await response.Content.ReadAsStreamAsync(cancel);
            using var document = await JsonDocument.ParseAsync(stream, cancellationToken: cancel);

            var roleIds = new HashSet<ulong>();
            if (document.RootElement.TryGetProperty("roles", out var rolesElement))
            {
                foreach (var roleElement in rolesElement.EnumerateArray())
                {
                    var roleIdRaw = roleElement.GetString();
                    if (ulong.TryParse(roleIdRaw, out var roleId))
                        roleIds.Add(roleId);
                }
            }

            return new DiscordGuildMemberRoles(true, roleIds);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception e)
        {
            _sawmill.Error("Discord guild member lookup for user {DiscordUserId} failed: {Exception}", discordUserId, e);
            return null;
        }
    }

    #endregion
}
