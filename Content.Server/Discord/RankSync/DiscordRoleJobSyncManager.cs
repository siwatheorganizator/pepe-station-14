using System.Linq;
using System.Security.Cryptography;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Content.Server.Chat.Managers;
using Content.Server.Database;
using Content.Server.Discord.DiscordLink;
using Content.Server.Players.JobWhitelist;
using Content.Shared.CCVar;
using Content.Shared.Roles;
using Robust.Server.Player;
using Robust.Shared.Configuration;
using Robust.Shared.ContentPack;
using Robust.Shared.Network;
using Robust.Shared.Player;
using Robust.Shared.Prototypes;
using Robust.Shared.Timing;
using Robust.Shared.Utility;
using DiscordLinkService = Content.Server.Discord.DiscordLink.DiscordLink;

namespace Content.Server.Discord.RankSync;

public sealed class DiscordRoleJobSyncManager : IPostInjectInit
{
    private static readonly ResPath LinkStorePath = new("/discord_rank_links.json");
    private static readonly JsonSerializerOptions JsonOptions = new() { WriteIndented = true };
    private static readonly TimeSpan LinkCodeLifetime = TimeSpan.FromMinutes(30);

    [Dependency] private readonly IConfigurationManager _cfg = default!;
    [Dependency] private readonly IChatManager _chat = default!;
    [Dependency] private readonly DiscordLinkService _discordLink = default!;
    [Dependency] private readonly ILogManager _logManager = default!;
    [Dependency] private readonly IPlayerManager _players = default!;
    [Dependency] private readonly IPrototypeManager _prototypes = default!;
    [Dependency] private readonly IResourceManager _resource = default!;
    [Dependency] private readonly IServerDbManager _db = default!;
    [Dependency] private readonly JobWhitelistManager _jobWhitelist = default!;
    [Dependency] private readonly UserDbDataManager _userDb = default!;

    private readonly object _configLock = new();
    private Dictionary<NetUserId, ulong> _configuredAccountLinks = new();
    private Dictionary<NetUserId, ulong> _linkedAccounts = new();
    private readonly Dictionary<string, PendingLink> _pendingLinksByCode = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<NetUserId, string> _pendingCodesByPlayer = new();
    private Dictionary<ulong, string[]> _roleMap = new();
    private HashSet<string> _managedJobs = new(StringComparer.Ordinal);

    private ISawmill _sawmill = default!;
    private bool _enabled;
    private bool _removeStale = true;
    private bool _onlineSyncRunning;
    private TimeSpan _syncInterval = TimeSpan.FromMinutes(5);
    private TimeSpan _timeUntilSync = TimeSpan.FromMinutes(5);

    public void Initialize()
    {
        LoadLinkedAccounts();

        _cfg.OnValueChanged(CCVars.DiscordRankSyncEnabled, OnEnabledChanged, true);
        _cfg.OnValueChanged(CCVars.DiscordRankSyncRemoveStale, OnRemoveStaleChanged, true);
        _cfg.OnValueChanged(CCVars.DiscordRankSyncAccountLinks, OnAccountLinksChanged, true);
        _cfg.OnValueChanged(CCVars.DiscordRankSyncRoleMap, OnRoleMapChanged, true);
        _cfg.OnValueChanged(CCVars.DiscordRankSyncInterval, OnSyncIntervalChanged, true);

        _discordLink.RegisterCommandCallback(OnDiscordLinkCommand, "link");
    }

    public void Shutdown()
    {
        _cfg.UnsubValueChanged(CCVars.DiscordRankSyncEnabled, OnEnabledChanged);
        _cfg.UnsubValueChanged(CCVars.DiscordRankSyncRemoveStale, OnRemoveStaleChanged);
        _cfg.UnsubValueChanged(CCVars.DiscordRankSyncAccountLinks, OnAccountLinksChanged);
        _cfg.UnsubValueChanged(CCVars.DiscordRankSyncRoleMap, OnRoleMapChanged);
        _cfg.UnsubValueChanged(CCVars.DiscordRankSyncInterval, OnSyncIntervalChanged);
    }

    public void Update(FrameEventArgs frameEventArgs)
    {
        if (!_enabled || _onlineSyncRunning || _syncInterval <= TimeSpan.Zero)
            return;

        _timeUntilSync -= TimeSpan.FromSeconds(frameEventArgs.DeltaSeconds);
        if (_timeUntilSync > TimeSpan.Zero)
            return;

        _timeUntilSync = _syncInterval;
        _onlineSyncRunning = true;
        _ = SyncOnlinePlayers();
    }

    private void OnPlayerLoaded(ICommonSession session)
    {
        if (!_enabled)
            return;

        if (!HasAccountLink(session.UserId))
            SendLinkCode(session);

        _ = SyncPlayerSafe(session.UserId, CancellationToken.None);
    }

    private async Task SyncOnlinePlayers()
    {
        try
        {
            foreach (var session in _players.Sessions.ToArray())
            {
                await SyncPlayerSafe(session.UserId, CancellationToken.None);
            }
        }
        finally
        {
            _onlineSyncRunning = false;
        }
    }

    private async Task SyncPlayerSafe(NetUserId player, CancellationToken cancel)
    {
        try
        {
            await SyncPlayer(player, cancel);
        }
        catch (OperationCanceledException)
        {
        }
        catch (Exception e)
        {
            _sawmill.Error("Discord rank sync failed for player {Player}: {Exception}", player, e);
        }
    }

    private async Task SyncPlayer(NetUserId player, CancellationToken cancel)
    {
        if (!_enabled)
            return;

        ulong discordUserId;
        Dictionary<ulong, string[]> roleMap;
        HashSet<string> managedJobs;

        lock (_configLock)
        {
            if (_roleMap.Count == 0)
                return;

            if (!_configuredAccountLinks.TryGetValue(player, out discordUserId) &&
                !_linkedAccounts.TryGetValue(player, out discordUserId))
            {
                return;
            }

            roleMap = _roleMap.ToDictionary(pair => pair.Key, pair => pair.Value);
            managedJobs = new HashSet<string>(_managedJobs, StringComparer.Ordinal);
        }

        var memberRoles = await _discordLink.GetGuildMemberRolesAsync(discordUserId, cancel);
        if (memberRoles == null)
            return;

        var desiredJobs = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (discordRoleId, jobs) in roleMap)
        {
            if (!memberRoles.RoleIds.Contains(discordRoleId))
                continue;

            foreach (var job in jobs)
            {
                desiredJobs.Add(job);
            }
        }

        var currentJobs = (await _db.GetJobWhitelists(player, cancel)).ToHashSet(StringComparer.Ordinal);
        var added = new List<string>();
        var removed = new List<string>();

        foreach (var job in desiredJobs)
        {
            if (currentJobs.Contains(job))
                continue;

            await _db.AddJobWhitelist(player, new ProtoId<JobPrototype>(job));
            added.Add(job);
        }

        if (_removeStale)
        {
            foreach (var job in managedJobs)
            {
                if (!currentJobs.Contains(job) || desiredJobs.Contains(job))
                    continue;

                await _db.RemoveJobWhitelist(player, new ProtoId<JobPrototype>(job));
                removed.Add(job);
            }
        }

        if (added.Count == 0 && removed.Count == 0)
            return;

        await _jobWhitelist.ReloadWhitelist(player, cancel);

        var memberState = memberRoles.IsGuildMember ? "guild member" : "not in guild";
        _sawmill.Info(
            "Synced Discord rank jobs for player {Player} ({MemberState}, Discord {DiscordUserId}). Added: {Added}; removed: {Removed}.",
            player,
            memberState,
            discordUserId,
            string.Join(", ", added),
            string.Join(", ", removed));
    }

    private void OnAccountLinksChanged(string rawLinks)
    {
        var links = new Dictionary<NetUserId, ulong>();

        foreach (var entry in SplitEntries(rawLinks))
        {
            if (!TrySplitEntry(entry, out var ss14UserIdRaw, out var discordUserIdRaw))
            {
                _sawmill.Warning("Ignoring malformed Discord rank sync account link '{Entry}'.", entry);
                continue;
            }

            if (!Guid.TryParse(ss14UserIdRaw, out var ss14UserId))
            {
                _sawmill.Warning("Ignoring Discord rank sync account link with invalid SS14 user id '{UserId}'.", ss14UserIdRaw);
                continue;
            }

            if (!ulong.TryParse(discordUserIdRaw, out var discordUserId))
            {
                _sawmill.Warning("Ignoring Discord rank sync account link with invalid Discord user id '{DiscordUserId}'.", discordUserIdRaw);
                continue;
            }

            links[new NetUserId(ss14UserId)] = discordUserId;
        }

        lock (_configLock)
        {
            _configuredAccountLinks = links;
        }
    }

    private void OnEnabledChanged(bool enabled)
    {
        _enabled = enabled;
    }

    private void OnRemoveStaleChanged(bool removeStale)
    {
        _removeStale = removeStale;
    }

    private void OnRoleMapChanged(string rawMap)
    {
        var roleMap = new Dictionary<ulong, string[]>();
        var managedJobs = new HashSet<string>(StringComparer.Ordinal);

        foreach (var entry in SplitEntries(rawMap))
        {
            if (!TrySplitEntry(entry, out var discordRoleIdRaw, out var jobsRaw))
            {
                _sawmill.Warning("Ignoring malformed Discord rank sync role map entry '{Entry}'.", entry);
                continue;
            }

            if (!ulong.TryParse(discordRoleIdRaw, out var discordRoleId))
            {
                _sawmill.Warning("Ignoring Discord rank sync role map entry with invalid Discord role id '{RoleId}'.", discordRoleIdRaw);
                continue;
            }

            var jobs = new List<string>();
            foreach (var jobId in jobsRaw.Split(new[] { ',', '|' }, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries))
            {
                var job = new ProtoId<JobPrototype>(jobId);
                if (!_prototypes.TryIndex(job, out var prototype))
                {
                    _sawmill.Warning("Ignoring Discord rank sync role map job '{JobId}' because no job prototype exists.", jobId);
                    continue;
                }

                if (!prototype.Whitelisted)
                    _sawmill.Warning("Discord rank sync maps Discord role {RoleId} to job {JobId}, but the job is not whitelisted.", discordRoleId, jobId);

                jobs.Add(jobId);
                managedJobs.Add(jobId);
            }

            if (jobs.Count > 0)
                roleMap[discordRoleId] = jobs.Distinct(StringComparer.Ordinal).ToArray();
        }

        lock (_configLock)
        {
            _roleMap = roleMap;
            _managedJobs = managedJobs;
        }
    }

    private void OnSyncIntervalChanged(float seconds)
    {
        _syncInterval = seconds <= 0f ? TimeSpan.Zero : TimeSpan.FromSeconds(seconds);
        _timeUntilSync = _syncInterval;
    }

    private bool HasAccountLink(NetUserId player)
    {
        lock (_configLock)
        {
            return _configuredAccountLinks.ContainsKey(player) || _linkedAccounts.ContainsKey(player);
        }
    }

    private void SendLinkCode(ICommonSession session)
    {
        var code = GetOrCreateLinkCode(session.UserId);
        var message =
            $"Discord link code: {code}. Send `{_discordLink.BotPrefix}link {code}` to the Discord bot to sync your Discord roles.";

        _chat.DispatchServerMessage(session, message, suppressLog: true);
    }

    private string GetOrCreateLinkCode(NetUserId player)
    {
        lock (_configLock)
        {
            var now = DateTimeOffset.UtcNow;
            PruneExpiredLinks(now);

            if (_pendingCodesByPlayer.TryGetValue(player, out var existingCode) &&
                _pendingLinksByCode.TryGetValue(existingCode, out var pending) &&
                pending.ExpiresAt > now)
            {
                return existingCode;
            }

            string code;
            do
            {
                code = GenerateLinkCode();
            }
            while (_pendingLinksByCode.ContainsKey(code));

            _pendingCodesByPlayer[player] = code;
            _pendingLinksByCode[code] = new PendingLink(player, now + LinkCodeLifetime);
            return code;
        }
    }

    private void OnDiscordLinkCommand(CommandReceivedEventArgs ev)
    {
        _ = HandleDiscordLinkCommand(ev);
    }

    private async Task HandleDiscordLinkCommand(CommandReceivedEventArgs ev)
    {
        if (ev.Message.Author.IsBot)
            return;

        if (!_enabled)
        {
            await SendDiscordReply(ev, "Discord role sync is disabled on the game server.");
            return;
        }

        if (ev.Arguments.Count != 1)
        {
            await SendDiscordReply(ev, $"Usage: `{_discordLink.BotPrefix}link <code>`.");
            return;
        }

        var code = NormalizeLinkCode(ev.Arguments[0]);
        if (!TryConsumePendingLink(code, out var player))
        {
            await SendDiscordReply(ev, "That link code is invalid or expired. Join the game server again to get a fresh code.");
            return;
        }

        var discordUserId = ev.Message.Author.Id;
        lock (_configLock)
        {
            foreach (var (linkedPlayer, linkedDiscordUserId) in _linkedAccounts.ToArray())
            {
                if (linkedDiscordUserId == discordUserId && linkedPlayer != player)
                    _linkedAccounts.Remove(linkedPlayer);
            }

            _linkedAccounts[player] = discordUserId;
        }

        SaveLinkedAccounts();

        await SendDiscordReply(ev, "Linked your Discord account to your SS14 account. Your game role whitelist is syncing now.");

        if (_players.TryGetSessionById(player, out var session))
            _chat.DispatchServerMessage(session, "Discord account linked. Your role whitelist is syncing now.", suppressLog: true);

        await SyncPlayerSafe(player, CancellationToken.None);
    }

    private bool TryConsumePendingLink(string code, out NetUserId player)
    {
        lock (_configLock)
        {
            PruneExpiredLinks(DateTimeOffset.UtcNow);

            if (!_pendingLinksByCode.Remove(code, out var pending))
            {
                player = default;
                return false;
            }

            if (_pendingCodesByPlayer.TryGetValue(pending.Player, out var playerCode) &&
                string.Equals(playerCode, code, StringComparison.OrdinalIgnoreCase))
            {
                _pendingCodesByPlayer.Remove(pending.Player);
            }

            player = pending.Player;
            return true;
        }
    }

    private void PruneExpiredLinks(DateTimeOffset now)
    {
        foreach (var (code, pending) in _pendingLinksByCode.ToArray())
        {
            if (pending.ExpiresAt > now)
                continue;

            _pendingLinksByCode.Remove(code);
            if (_pendingCodesByPlayer.TryGetValue(pending.Player, out var playerCode) &&
                string.Equals(playerCode, code, StringComparison.OrdinalIgnoreCase))
            {
                _pendingCodesByPlayer.Remove(pending.Player);
            }
        }
    }

    private async Task SendDiscordReply(CommandReceivedEventArgs ev, string message)
    {
        await _discordLink.SendMessageAsync(ev.Message.ChannelId, message);
    }

    private void LoadLinkedAccounts()
    {
        if (!_resource.UserData.TryReadAllText(LinkStorePath, out var raw))
            return;

        Dictionary<string, string>? storedLinks;
        try
        {
            storedLinks = JsonSerializer.Deserialize<Dictionary<string, string>>(raw);
        }
        catch (JsonException e)
        {
            _sawmill.Error("Unable to parse Discord rank sync link store {Path}: {Exception}", LinkStorePath, e);
            return;
        }

        if (storedLinks == null)
            return;

        var links = new Dictionary<NetUserId, ulong>();
        foreach (var (ss14UserIdRaw, discordUserIdRaw) in storedLinks)
        {
            if (!Guid.TryParse(ss14UserIdRaw, out var ss14UserId) ||
                !ulong.TryParse(discordUserIdRaw, out var discordUserId))
            {
                _sawmill.Warning("Ignoring malformed Discord rank sync stored link '{UserId}' = '{DiscordUserId}'.",
                    ss14UserIdRaw,
                    discordUserIdRaw);
                continue;
            }

            links[new NetUserId(ss14UserId)] = discordUserId;
        }

        lock (_configLock)
        {
            _linkedAccounts = links;
        }
    }

    private void SaveLinkedAccounts()
    {
        Dictionary<string, string> storedLinks;
        lock (_configLock)
        {
            storedLinks = _linkedAccounts
                .OrderBy(pair => pair.Key.UserId)
                .ToDictionary(
                    pair => pair.Key.UserId.ToString(),
                    pair => pair.Value.ToString(),
                    StringComparer.Ordinal);
        }

        var raw = JsonSerializer.Serialize(storedLinks, JsonOptions);
        _resource.UserData.WriteAllText(LinkStorePath, raw);
    }

    private static string GenerateLinkCode()
    {
        Span<byte> bytes = stackalloc byte[4];
        RandomNumberGenerator.Fill(bytes);
        var value = BitConverter.ToUInt32(bytes);
        return (value % 1_000_000).ToString("D6");
    }

    private static string NormalizeLinkCode(string code)
    {
        return code.Trim().Replace("-", string.Empty).ToUpperInvariant();
    }

    private static IEnumerable<string> SplitEntries(string raw)
    {
        return raw.Split(new[] { ';', '\n', '\r' }, StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }

    private static bool TrySplitEntry(string entry, out string left, out string right)
    {
        var separator = entry.IndexOf('=');
        if (separator < 0)
            separator = entry.IndexOf(':');

        if (separator <= 0 || separator >= entry.Length - 1)
        {
            left = string.Empty;
            right = string.Empty;
            return false;
        }

        left = entry[..separator].Trim();
        right = entry[(separator + 1)..].Trim();
        return true;
    }

    void IPostInjectInit.PostInject()
    {
        _sawmill = _logManager.GetSawmill("discord.rank_sync");
        _userDb.AddOnFinishLoad(OnPlayerLoaded);
    }

    private sealed record PendingLink(NetUserId Player, DateTimeOffset ExpiresAt);
}
