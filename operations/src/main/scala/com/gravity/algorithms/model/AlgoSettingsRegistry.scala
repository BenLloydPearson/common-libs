package com.gravity.algorithms.model

import com.gravity.interests.jobs.intelligence.operations.{ProbabilityDistribution, ValueProbability}

import scala.collection.mutable

/**
 * A registry of algo settings.  This should be extended into a singleton object -- if not the register function will not be thread safe!
 */
class AlgoSettingsRegistry extends AlgoSettingsOps {

  private val _registry: mutable.Buffer[AlgoSetting[_]] = scala.collection.mutable.Buffer[AlgoSetting[_]]()

  private def register[T <: AlgoSetting[_]](setting: T): T = {
    _registry += setting
    setting
  }

  def registry: Seq[AlgoSetting[_]] = _registry.toSeq

  def settingByName(name: String): Option[AlgoSetting[_]] = _registry.find(_.name == name)

  val recoGenServersForSite: Variable = register(Variable("recogen-servers-for-site", "recoGen", 1.0, "This is the number of servers used to process a given siteGuid"))

  // transient settings - persisted but removed prior to returing reco result for rendering
  val alphaSetting: TransientSetting = register(TransientSetting("alpha-setting","beta-distribution","","represents the alpha value for a beta distribution calculation", true))
  val betaSetting: TransientSetting = register(TransientSetting("beta-setting","beta-distribution","","represents the beta value for a beta distribution calculation", true))


  // Reco reason
  val recoSelectionReasonWhy: Setting = register(Setting("reco-why","Reco Reason","","represents reason why the reco was selected"))
  val recoSelectionReasonWhyMeta: Setting = register(Setting("reco-why-meta","Reco Reason","","represents meta-data in the form of scopedKeys for reason why the reco was selected."))

  // Placements
  val useClickUrlForDisplayUrl: Switch = register(Switch("use-click-url-for-display-url", "Widget", default = false,
    "TRUE to let widgets use campaign-article clickUrl as the URL displayed to the user (as opposed to the default, article canonical URL."))
  val infiniteScroll: Switch = register(Switch("infinite-scroll", "Widget", default = false,
    "TRUE to enable the widget for infinite scroll"))
  val interstitialUrl: Setting = register(Setting("interstitial-url", "Widget", default = "",
    "If provided, article clicks for the given placement will go to this URL with an added query string parameter `cak` indicating the clicked article; see PrependCampaignArticleKeyServlet for details"))

  //Adding lines to test build issue
  val recoSelectionReasonSettingNames = Set(recoSelectionReasonWhy.name, recoSelectionReasonWhyMeta.name)

  //Storage system
  val storage_useSystem: Switch = register(Switch("use-reco-storage", "Storage", true, "If true, attempt to fetch recos from new storage system"))
  val storage_minImpressionsForContextualRecoPreGen: Variable = register(Variable("min-impressions-contextual-reco-pregen", "Storage", 1000, "This is the minimum impressions an article must have for contextual recos to be pre-generated"))
  val generationAlgoIds: Setting = register(Setting("generation-algo-ids","Storage","","A comma delimited list of algoIds used to generate recos"))
  val retrievalAlgoIds: Setting = register(Setting("retrieval-algo-ids","Storage","","A comma delimited list of algoIds used to retrieve recos"))
  val enableRemoteRecoFetch: Switch = register(Switch("enable-remote-reco-fetch", "Storage", false, "If true, attempt to fetch recos via a remoting call"))
  val runRecoFetchStrategyLocally: Switch = register(Switch("run-reco-fetch-strategy-locally", "Reco Storage", false, "If true, attempt to fetch recos locally use reco fetch strategy"))
  val maxRecoResultsToRead: Variable = register(Variable("max-reco-results-to-read","Storage", 60 ,"Max number of recos to read from storage"))
  val remoteRecoFetchThreadPoolSize: Variable = register(Variable("remoteRecoFetchThreadPoolSize","Storage", 5 ,"Number of threads to use in pool to read from storage"))
  val maxRecoResultsToWrite: Variable = register(Variable("max-reco-results-to-write","Storage", 60 ,"Max number of recos to write to storage"))
  val enablePersonalizedRecoFetch: Switch = register(Switch("enable-personalized-reco-fetch", "Storage", true, "If false, no attempt will be made to fetch contextual or personalized recos"))
  val performUserClickstreamAnalysis: Switch = register(Switch("perform-user-clickstream-analysis", "Storage", false, "If true, analyze user clickstream and increment appropriate counters"))
  val remoteRecoFetchStrategy: Variable = register(Variable("remote-reco-fetch-strategy","Storage", 2 ,"Strategy used to create final reco result.  See RecommenderStrategy Type for available options.", true))
  val personalizationStrategy_minClickIntersection: Variable = register(Variable("personalization-strategy-min-click-intersection","Storage", 2,"The minimal number of articles in a user's click-stream that must yield a non-empty reco result", true))
  val personalizationStrategy_maxMinutesOld: Variable = register(Variable("personalization-strategy-max-minutes-old","Storage", 480, "Will use recos from storage if below this threshold"))
  val personalizationStrategy_sortBy: Variable = register(Variable("personalization-strategy-sort-by","Storage", 1, "Used to customize the sort of the final result.  See PersonalizationStrategy_SortType"))
  val reco_maxMinutesOld: Variable = register(Variable("reco-max-minutes-old","Storage", 240, "Will use recos from storage if below this threshold"))
  val enableStoragePermaCache: Switch = register(Switch("enable-storage-permacacher", "Storage", false, "If false, storage results will not be permacached"))
  val forceDisabledRecoStrategies: Setting = register(Setting("force-disabled-reco-strategies","Storage","","Represents a comma delimited list of reco strategy ids that are NOT allowed to execute. The default strategy will be executed in it's place."))

  val minClickStreamSizeForSemanticArticleClustering: Variable = register(Variable("min-clickstream-size-for-semantic-article-clustering","Storage", 3,"The minimal number of articles in a user's click-stream that is required to attempt personalization via an semantic article clustering algo", true))
  val minClickStreamSizeForUserClustering: Variable = register(Variable("min-clickstream-size-for-user-clustering","Storage", 2,"The minimal number of articles in a user's click-stream that is required to attempt personalization via a user clustering algo", true))
  val attemptContextualRecos: Switch = register(Switch("attempt-contextual-recos", "Storage", false, "If true, strategy will attempt to return contextual recos"))

  val mustReturnAtLeast: Variable = register(Variable("must-return-at-least", "Result Rules", 0, "If above zero, the algo must return at least that many values"))

  // GMS Settings
  val gmsAdUnitSlot : Variable = register(Variable("gms-ad-unit-slot" , "GMS", -1, "The slot where the first ad will appear in the placement."))
  val gmsAdUnitSlot2: Variable = register(Variable("gms-ad-unit-slot2", "GMS", -1, "The slot where the second ad will appear in the placement."))
  val gmsMinArticles: Variable = register(Variable("gms-min-articles" , "GMS", -1, "Minimum number of articles required by the placement."))
  val gmsForceUsersToSlide1: Variable = register(Variable("force-users-to-slide1", "GMS", 0, "Seconds timestamp when an admin last requested 'force users to slide 1.'"))
  val gmsDlugInMaintenanceMode: Switch = register(Switch("dlug-maintenance-mode", "GMS", false, "If this is true, we are in Aol.com DLUG-Transition mode, and should try to be quiescent.", true))
  val gmsDlugUsesMultisiteGms: Switch = register(Switch("dlug-uses-multisite-gms", "GMS", false, "If this is true, Aol.com DLUG should use the new multi-site GMS.", true))
  val gmsDataLayerDlChannel: Setting = register(Setting("gms-data-layer-dl-ch", "GMS", "", "APPLY to ContentGroupIds: Identifies the DL Channel value used by AOL's DataLayer services."))
  val gmsRequirePlid: Switch = register(Switch("gms-require-gravityCalculatedPlid", "GMS", false, "If this is true, ALL articles returned in StaticWidgets must have one set in Grv:Map.", true))
  val gmsImageInDataField: Switch = register(Switch("gms-image-in-data-field", "GMS", false, "If this is true, ALL articles returned in StaticWidgets must have their image located in the Grv:Map.", true))

  // API specific settings
  val reloadCacheAPI: Switch = register(Switch("reload-cach-on-api", "API cache", false, "If true, cache manager will reload article/metric cache periodically"))

  //Retrieval settings
  val useDithering: Switch = register(Switch("dither-results", "Retrieval", false, "If true, will dither the results of all reco fetches", true))
  val ditherSkew: Variable = register(Variable("dither-skew", "Retrieval", 0.5, "A higher value means more of a chance for mixing in the results", true))
  val enableRemoteLiveFallback: Switch = register(Switch("enable-remote-live-fallback", "Retrieval", false, "Set this to true if you want to enable remote live fallbacks"))
  val failIfNotEnoughRecos: Switch = register(Switch("fail-if-not-enough-recos", "Retrieval", true, "Set this to true if you want to fail if not enough recos are retrieved, or false to return whatever recos were fetched"))
  val failIfNoRecos: Switch = register(Switch("fail-if-no-recos", "Retrieval", true, "Set this to true if you want to fail if zero recos were retrieved, or false to return an empty result"))
  val enableTitleDeDupOnFetch: Switch = register(Switch("enable-title-dedup-on-fetch", "Retrieval", true, "If true, will dedup recos based on title during reco fetches"))

  // Live Recos
  val liveRecosTimeoutMillis: Variable = register(Variable("live-recos-timeout-millis", "LiveRecos", 300, "This specifies the amount of time a live recos thread is allowed to execute before fallbacks are returned", false))
  val liveRecosWarmingRequestIntervalSeconds: Variable = register(Variable("live-recos-warming-request-interval-seconds", "LiveRecos", 0, "This specifies how often a request will be fired to warm the server", false))
  val liveRecosWarmingRequestEnabled: Switch = register(Switch("live-recos-warming-request-enabled", "LiveRecos", true, "If true, will make periodic reco requests"))


  // Sim Score Algo
  val simScoreCompareThreshold: Variable = register(Variable("sim-score-compare-threshold", "SimScore Algo", 0.1, "This is the simScore threshold that must be met to use simScore based results"))
  val simScoreAllSiteGraph: Switch = register(Switch("sim-score-all-site-graph", "SimScore Algo", false, "Set this to true if you want to combine a users graph from all the sites they have been to"))
  val minTfidfScoreForTagGraphing: Variable = register(Variable("min-tfidf-score-for-tag-graphing", "SimScore Algo", 0.0, "This is the simScore threshold that must be met to use nodes in a taggraph"))


  // New cumulative probability distribution
  val probabilityDistributionTest: ProbabilityDistributionSetting = register(
    ProbabilityDistributionSetting("test-prob-distribution", "Test Prob Distribution",
      ProbabilityDistribution(List(ValueProbability(1.0, 30), ValueProbability(2.0, 100))), "This is a split test across 2 weighted buckets"))

  // RecentClickstreamSimilarityScoreBundle
  val maxClickstreamDepth: Variable = register(Variable("max-clickstream-depth", "RecentClickstreamSimilarityScoreBundle", 10, "The max number of recent articles to consider in a user's clickstream"))


  // auto yo
  val enableAutoYO: Switch = register(Switch("enable-auto-yo", "auto yo", true, "Set this to true if you want to use auto yo probability distributions", true))

  val yoArticleImpressionFloorPDS: ProbabilityDistributionSetting = register(
    ProbabilityDistributionSetting("article-impression-floor-prob", "auto yo",
      ProbabilityDistribution(List(
        ValueProbability(3000, 70), ValueProbability(2500, 85), ValueProbability(3500, 100))),
      "Various possible values for article impression floor"))

  val yoChurnerPDS: ProbabilityDistributionSetting = register(
    ProbabilityDistributionSetting("yo-churner-prob", "auto yo",
      ProbabilityDistribution(List(ValueProbability(0 /*churner*/ , 50), ValueProbability(1 /*yo*/ , 100))),
      "partition traffic across churner and yo"))

  val churnerDemocraticChurnerPDS: ProbabilityDistributionSetting = register(
    ProbabilityDistributionSetting("churner-democratic-churner-prob", "auto yo",
      ProbabilityDistribution(List(ValueProbability(0 /*churner*/ , 85), ValueProbability(2 /*democratic churner*/ , 100))),
      "partition traffic across churner & democratic churner"))


  val yoChurnerMinChurnRate: Variable = register(Variable("churn-min-rate", "auto yo", 0, "An auto tuned yo-churner algo will not go below this churn rate"))
  val yoChurnerMaxChurnRate: Variable = register(Variable("churn-max-rate", "auto yo", 15, "An auto tuned yo-churner algo will not go above this churn rate"))
  val yoBundles: Variable = register(Variable("yo-bundles", "auto yo", 1, "The number of distinct bundles to create and choose from when running YO"))

  // Ordinal Settings
  val impressionCapOrdinal0: Variable = register(Variable("impression-cap-ordinal0", "Ordinal", 50, "Articles are churned at ordinal0 for this many impressions"))
  val impressionCapOrdinal1: Variable = register(Variable("impression-cap-ordinal1", "Ordinal", 50, "Articles are churned at ordinal1 for this many impressions"))
  val ctrWeightOrdinal0: Variable = register(Variable("ctr-weight-ordinal0", "Ordinal", 2, "The weight of ctr at ordinal0"))
  val ctrWeightOrdinal1: Variable = register(Variable("ctr-weight-ordinal1", "Ordinal", 1, "The weight of ctr at ordinal1"))
  val ctrWeightTailOrdinals: Variable = register(Variable("ctr-weight-tail-ordinals", "Ordinal", 0.5, "The combined weight of ctr at ordinals 2 thru last ordinal"))
  val useImpressionsOverHoursForMetrics: Switch = register(Switch("use-impressions-for-metrics", "Ordinal", true, "If true, will use last x impressions otherwise last x hours for calculating metrics"))
  val maxOrdinals: Variable = register(Variable("max-ordinals", "Ordinal", -1, "if greater that -1, will use tail ordinals from 2 up to this number to calculate ctr"))
  val useImpressionsViewedYO: Switch = register(Switch("use-impressions-viewed-yo", "Ordinal", false, "If true, will use impressions viewed instead of impressions for yo"))
  val useImpressionsViewedChurner: Switch = register(Switch("use-impressions-viewed-churner", "Ordinal", false, "If true, will use impressions viewed instead of impressions for churner"))

  //Auto Balance Algo Settings
  val personalizedAlgoAutoBalanceRatio: Variable = register(Variable("personalized-algo-ratio", "AutoBalance Algo", 50, "This is the percentage of traffic dedicated to a personalized algo"))
  val contextualAlgoAutoBalanceRatio: Variable = register(Variable("contextual-algo-ratio", "AutoBalance Algo", 50, "This is the percentage of traffic dedicated to a contextual algo"))
  val enableAutoBalance: Switch = register(Switch("auto-balance-enabled", "AutoBalance Algo", true, "If turned on, algos will attempt to auto balance the ratio on their own"))
  val dailyAutoBalanceFrequency: Variable = register(Variable("daily-auto-balance-frequency", "AutoBalance Algo", 12, "This is the number of times the ratios will be updated per day"))
  val enableAutoResetBalance: Switch = register(Switch("auto-balance-reset-enabled", "AutoBalance Algo", false, "If turned on, algos will attempt to reset the ratio on their own"))
  val weeklyAutoBalanceResetFrequency: Variable = register(Variable("weekly-auto-balance-reset-frequency", "AutoBalance Algo", 7, "This is the number of times the ratios will be reset per week"))
  val lastAutoBalanceTime: Variable = register(Variable("last-auto-balance-time", "AutoBalance Algo", -1, "This is the last time  an auto balance was performed"))
  val testModeAutoBalance: Switch = register(Switch("test-mode-auto-balance", "AutoBalance Algo", false, "If turned on, algo will run but not update the ratios"))
  val lookbackHoursAutoBalance: Variable = register(Variable("lookback-hours-auto-balance", "AutoBalance Algo", 8, "This is the number of hours to look back when aggregating scoped metrics"))

  //Candidate Settings
  val minCpcPenniesMobileUS: Variable = register(Variable("min-cpc-pennies-mobile-us","Candidate Settings", 0 ,"Min cpc for sponsored content mobile us"))
  val minCpcPenniesDesktopUS: Variable = register(Variable("min-cpc-pennies-desktop-us","Candidate Settings", 0 ,"Min cpc for sponsored content desktop us"))
  val minCpcPenniesMobileIntl: Variable = register(Variable("min-cpc-pennies-mobile-intl","Candidate Settings", 0 ,"Min cpc for sponsored content mobile international"))
  val minCpcPenniesDesktopIntl: Variable = register(Variable("min-cpc-pennies-desktop-intl","Candidate Settings", 0 ,"Min cpc for sponsored content desktop international"))

  val maxDaysOld: Variable = register(Variable("max-days-old", "Candidate Settings", 7300, "If a candidates' publish date is older than this, discard", true))
  val cacheCandidates: Switch = register(Switch("cache-candidates", "Candidate Settings", false, "If turned on, candidate set will be cached"))
  val maxCandidateSize: Variable = register(Variable("max-candidate-size", "Candidate Settings", 15000, "The max number of articles a candidate provider will return"))
  val cacheCandidateMinutes: Variable = register(Variable("cache-candidate-minutes", "Candidate Settings", 12, "if caching is enabled, a  candidate provider will cache a candidate set for the specified minutes"))
  val requireImages: Switch = register(Switch("require-images", "Candidate Settings", false, "If this is true, then require that all recommendations have images"))
  val requireContent: Switch = register(Switch("require-content", "Candidate Settings", false, "If this is true, then require that all recommendations have content"))
  val suppressGifs: Switch = register(Switch("suppress-gifs", "Candidate Settings", true, "If this is true, then treat a GIF as an empty image for recos"))
  val filterOutNsfwTitles: Switch = register(Switch("filter-nsfw-titles", "Candidate Settings", true, "If this is true, then any artile with NSFW in the title will be filtered out"))
  val filterViaDemographics: Switch = register(Switch("filter-via-demographics", "Candidate Settings", true, "If this is true, articles will be filtered according to demographic settings"))

  val beaconMinimumForFilterBypassPds:  ProbabilityDistributionSetting = register(
    ProbabilityDistributionSetting("beacon-minimum-for-filter-bypass-prob", "Candidate Settings",
      ProbabilityDistribution(List(
        ValueProbability(1000, 85), ValueProbability(500, 94), ValueProbability(250, 99), ValueProbability(100, 100))),
      "Various possible values for minimum number of beacons that an article has to have in a metrics period in order to avoid quality filter"))


  val democraticChurner_maxImpressionsForSite: Variable = register(Variable("max-impressions-for-site", "Democratic Churner", 5000, "The max number of impressions to give to a campaign per site"))
  val democraticChurner_maxImpressionsForNetwork: Variable = register(Variable("max-impressions-for-network", "Democratic Churner", 10000, "The max number of impressions to give to a campaign across all sites"))
  val democraticChurner_minArticlesPerCampaignToChurn: Variable = register(Variable("min-articles-per-campaign-to-churn", "Democratic Churner", 300, "The min number of articles to churn per campaign"))

  //  val oneCampaignPerSlot = register(Switch("one-campaign-per-slot", false, "If turned to true, there will only be one campaign per slot in a sponsored placement"))
  val oneAdvertiserPerSlot: Switch = register(Switch("one-advitiser-per-slot", "Sponsored", true, "If turned to true, there will be only one of an advitiser's articles per slot in a sponsored placement"))
  val YO_articleImpressionFloor: Variable = register(Variable("article-impression-floor", "YO", 3000, "The number of impressions required before an article goes from a Churner to a YO algo"))
  val YO_articleImpressionsToCount: Variable = register(Variable("article-impressions-to-count","YO", -1, "The number of impressions used by the YO algorithm.  Will be the same as article-impression-floor if not overridden"))
  val YO_useMetricsAtLevel: Variable = register(Variable("use-metrics-at-level", "YO", 2, "Use metrics at one of these levels: 1=cross-site, 2=site, 3=placement.  See MetricsLevel"))

  val YO_articleMetricsHoursOldFloor: Variable = register(Variable("article-hours-old-floor", "YO", 96, "How many hours old an article must be to enter YO"))
  val YO_articleMetricsBucketId: Variable = register(Variable("YO_articleMetricsBucketId", "YO", -1, "What bucket an YO algorithm will peek into---1 means it will peek into all of them"))

  val YO_applyImpressionFilter: Switch = register(Switch("YO_applyImpressionFilter", "YO", true, "TBD"))
  val YO_CTRThreshold: Variable = register(Variable("YO_CTRThreshold", "YO", 0.1, "The CTR an article must have to enter YO consideration"))

  val YO_fallbackToPopularity: Switch = register(Switch("YO_fallbackToPopularity", "YO", false, "Fallback to popularity score if there are not enough articles that pass the CTR Threshold"))

  val YO_numberOfSlots: Variable = register(Variable("YO_number_of_slots", "YO", 8, "The number of slots in the placement. Currently used when certain slots have advantages over other slots and randomization is necessary."))

  val YO_pullCovisitationData: Switch = register(Switch("YO_pullCovisitationData", "covisitation", false, "if true covisitation data is pulled"))
  val YO_covisitationWindowHours: Variable = register(Variable("YO_covisitationWindowHours", "covisitation", 4, "covisitation data is pulled for the last x hours"))
  val YO_minCovisitationToCount: Variable = register(Variable("YO_minCovisitationToCount", "covisitation", 30, "covisitation data ignored if forward covisitation count is below this threshold"))
  val YO_minCovisitationUniqueToCount: Variable = register(Variable("YO_minCovisitationUniqueToCount", "covisitation", 10, "covisitation data ignored if forward uniqueue covisitation count is below this threshold"))
  val YO_covisitationBias: Variable = register(Variable("YO_covisitationBias", "covisitation", 0.5, "covisitation bias multitplier. For scale:  0 eliminates any bias, 0.5 yields a noticeable yet conservative bias and 1+ creates a large bias."))

  //Churner Settings
  val YO_churnerEjectionDiscountRate: Variable = register(Variable("YO_churner-ejection-discount-rate", "YO", .80, "The discount % to apply to impression thresholds before being ejected by a churner"))
  val YO_numberEjectionTraunches: Variable = register(Variable("YO_num-ejection-traunches", "YO", 4, "The number of ejection traunches to apply during churning"))
  val YO_pinArticlesByChurnPriority: Switch = register(Switch("YO_pin_articles", "YO", false, "If true will always respect the churnPriority data point in the Articles table, effectively pinning by that point"))

  // CloudSearch
  val useCloudSearchArticlesSchemaVersion: Variable = register(Variable("cloudsearch-articles-schema-version", "CloudSearch", 0,
    "The current version of the schema to use for CloudSearch articles.  Any updates are guaranteed to be using this version, or else are suppressed."))

  // METRICS
  val metrics_byHostSite: Switch = register(Switch("metrics_byHostSite", "metrics", false, "If set to true, CTR will be for entire site"))

  // CAMPAIGN AUDIT LIMITS
  val campaignAuditReportEnabled: Switch = register(Switch("campaign-audit-report-enabled", "Campaign Audit Report", false, "If set to true, DATAFEEDS will periodically run the CampaignAuditReport"))

  val recentArticleSizeAlertYellow: Variable = register(Variable("campaign-audit-size-alert-yellow", "Campaign Audit Report", 50 * 1000,
    "If a campaign has more than this many recent articles, a NOTICE e-mail will be sent by the CampaignAuditReport."))

  val recentArticleSizeAlertRed: Variable = register(Variable("campaign-audit-size-alert-red", "Campaign Audit Report", 100 * 1000,
    "If a campaign has more than this many recent articles, a WARNING e-mail will be sent by the CampaignAuditReport."))

  val recentArticleZombieSlack: Variable = register(Variable("campaign-audit-zombie-slack", "Campaign Audit Report", 99,
    "If a campaign has more than this many possible zombie articles, a NOTICE e-mail will be sent by the CampaignAuditReport."))

  // FILL CAMPAIGN
  val enableFillCampaign: Switch = register(Switch("enableFillCampaign", "Fill Campaign", false, "If set to true, will append articles with no geo restrictions to standard reco result"))
  val fillCampaignArticles: Variable = register(Variable("fill-campaign-articles", "Fill Campaign", 10, "The number of articles to append when fill campaign is enabled"))
  val fillCampaign_minCountryCodeCount: Variable = register(Variable("fill-campaign-min-country-code-count", "Fill Campaign", 8, "The min number of attached country/region codes required to qualify as a fill campaign article"))

  //FREQUENCY CAPPING
  val globalUseFrequencyCapping: Switch = register(Switch("global-use-frequency-capping", "Frequency Capping", true, "A switch to tunr it off completely for load testing, should only be set at EveryThingKey level"))
  val useFrequencyCapping: Switch = register(Switch("use-frequency-capping", "Frequency Capping", false, "If set to true, will turn on frequency capping", true))
  val frequencyCappingTimesInClickstream: Variable = register(Variable("frequency-capping-times-in-clickstream", "Frequency Capping", 1, "How many times an article can be in a user's clickstream to be capped", true))
  val frequencyCappingTimesClicked: Variable = register(Variable("frequency-capping-times-clicked", "Frequency Capping", 5, "How many times an article can be clicked on by a user before it is capped", true))
  val frequencyCappingTimesImpressed: Variable = register(Variable("frequency-capping-times-impressed", "Frequency Capping", 50, "How many times an article seen in recommendations by a user before it is capped", true))
  val frequencyCappingTimesViewed: Variable = register(Variable("frequency-capping-times-viewed", "Frequency Capping", 25, "How many times an article gets an impression viewed event before it is capped", true))

  // Campaign Level FREQUENCY CAPPING INTERESTS-7960
  val useCampaignFrequencyCapping: Switch = register(Switch("use-campaign-frequency-capping", "Campaign Frequency Capping", false, "If set to true, will turn on campaign frequency capping"))
  val campaignfrequencyCappingTimesImpressed: Variable = register(Variable("campaign-frequency-capping-times-impressed", "Campaign Frequency Capping", 5, "How many times an article can be seen in recommendations by a user before it is capped"))
  val campaignfrequencyCappingTimesImpressedHour: Variable = register(Variable("campaign-frequency-capping-times-impressed-hour", "Campaign Frequency Capping", 1, "How many hours the impression threshold is over"))

  //INTEREST GRAPHING
  val supportsInterestGraphTargeting: Switch = register(Switch("supports-interest-graph-targeting", "Interest Targeting", true, "If the site has enough metadata to support interest graph targeting.  Specifically, it has graphed articles and graphed users"))

  //HIGHLIGHTER
  val useDynamicClickStream: Switch = register(Switch("use-dynamic-click-stream", "Highlighter", false, "if enabled, user's last few clicks are graphed and included in the final score calculation"))

  //REDIRECT
  val redirectToAolAddCPSParam: Switch = register(Switch("redirect-to-aol-add-cps-param", "Redirect", false, "if enabled, redirects to aol urls wil add a cps parameter"))

  //REGRESSION
  val hoursForMetricsRegression: Variable = register(Variable("hours-for-metric-regression","Regression Scoring",5.0, "How many hours to go back for metrics regression.  As a guideline should be within the amount of time it takes for traffic to peak or trough."))

  //SPONSORED STORIES
  val pinPublishersByAlgo: Setting = register(Setting("pinned-publishers-by-algo","Sponsored","","A comma delimited list of publishers that must be pinned to the top of an algo"))

  //TOPIC MODELS
  val useArticleWordVectorClustering: Switch = register(Switch("use-article-word-vector-clustering", "Clustering", false, "if enabled, uses article clusters created using wordVector models"))
  val maxDistanceWordVectorClustering: Variable = register(Variable("max-distance-word-vector-clustering", "Clustering",.2,"The max allowed distance an article can be from a cluster center. Value can be from 0 to 1.0"))
  val wordVectorSemanticWeight: Variable = register(Variable("word-vector-semantic-weight", "Clustering",10.0,"The weight assigned to an article based on it's semantic similarity.  Increase to favor semantic similarity."))
  val wordVectorViewWeight: Variable = register(Variable("word-vector-view-weight", "Clustering",1.0,"The weight assigned to an article based on it's views.  Increase to favor popular content."))
  val wordVectorRecencyWeight: Variable = register(Variable("word-vector-recency-weight", "Clustering",1.0,"The weight assigned to an article based on it's recency.  Increase to favor recent content."))
  val wordVectorInterleavePopular: Switch = register(Switch("word-vector-interleave-popular", "Clustering", false, "if enabled, interleaves popular results in semantic results"))

  val clusterProfileToUse: Setting = register(Setting("cluster-profile","Clustering","tfidf-phrase-bigrams","The ClusterProfile to use, as defined in ClusteringProfiles"))
  val semanticClusterProfileToUse: Setting = register(Setting("semantic-cluster-profile","Clustering","tfidf-phrase-graph","The ClusterProfile to use for semantic clustering, as defined in ClusteringProfiles"))
  val baseUserClusteringProbability: Variable = register(Variable("required-user-clustering-probability", "Clustering",50.0,"The probability a user must be in a topic before they will be considered for fetch.", true))
  val baseUserClusteringMetricsCollectionProbability: Variable = register(Variable("required-user-clustering-metrics-collection-probability", "Clustering",50.0,"The probability a user must be in a topic before they will be considered for metrics collection and algos."))
  val baseArticleClusteringProbability: Variable = register(Variable("required-article-clustering-probability", "Clustering",10.00,"The probability an article must be in a topic before they will be considered by the algo.", true))
  val trainArticlesIntoClusterAtFetchTime: Switch = register(Switch("train-articles-into-cluster-at-fetchtime","Clustering",true,"If true, unknown articles will be trained into existing clusters and used by the reco strategy/algo"))
  val clusterOverrides: Setting = register(Setting("cluster-overrides", "Clustering","","Comma delimited list of clusters to force during algo run.  If not specified will use user's clickstream."))
  val onlyUseSemanticIfPopular: Switch = register(Switch("semantic-if-popular", "Clustering",true,"If true, only allow a semantic similarity to go through if there is some measure of popularity for an article"))
  val behavioralBackfullNonYOArticles: Switch = register(Switch("cluster-backfill-nonyo","Clustering",false,"If true, non YO popular articles will get backfilled via the behavioral clustering.  For very sparse click sites"))
  val baseClusterScoreForRetrieval: Variable = register(Variable("cluster-base-score", "Clustering",100.01,"The base score the result must have before it will be returned"))
  val clusterBaseImpressions: ProbabilityDistributionSetting = register(ProbabilityDistributionSetting("cluster-base-impressions", "Clustering", ProbabilityDistribution(
    List(
      ValueProbability(500.0, 100)
    )
  ), "The number of impressions required to be considered tested in the cluster"))

  val clusterLookbackImpressions: ProbabilityDistributionSetting = register(ProbabilityDistributionSetting("cluster-lookback-impressions", "Clustering", ProbabilityDistribution(
    List(
      ValueProbability(3000.0, 100)
    )
  ), "The number of impressions that will be peeked into"))

  val finalSemanticScoreName: Setting = register(Setting("finalSemanticScoreName", "Clustering","semanticScore","The final score to use from the worksheet. Should be a valid column in OrganicSemanticClusteredScoreBundle."))


  //API Response level Meta
  val apiExposeBaseline: Switch = register(Switch("api-expose-baseline", "API Meta", false, "Should we include a field in the API response that tells the consumer if the user is in a baseline bucket?"))
  val apiExposeDlAdUnitSlot: Switch = register(Switch("api-expose-dl-data-beacon", "API Meta", false, "Whether to expose AOL DL ad unit slot number"))
  val apiExposeDlAdUnitSlot2: Switch = register(Switch("api-expose-dl-ad-unit-2", "API Meta", false, "Whether to expose AOL DL ad unit slot 2 number"))
  val apiExposeGmsAdUnitSlot: Switch = register(Switch("api-expose-gms-ad-unit", "API Meta", false, "Whether to expose GMS ad unit slot(s)"))
  val apiExposeDlForceUsersToSlide1LastTouchedTime: Switch = register(Switch("api-expose-dl-force-users-to-slide1-last-touched-time", "API Meta", false, "Should we include an API field to expose DL 'force users to slide1 last touched time' setting?"))
  val apiExposeBucketId: Switch = register(Switch("api-expose-bucket-id", "API Meta", false, "Should we include an API field to expose which bucket a user is in?"))
  val apiExposeResponseHash: Switch = register(Switch("api-expose-response-hash", "API Meta", false, "Should we include an API field to expose a hash of the response fields (ex clickUrl)?"))
  val apiExposeServer: Switch = register(Switch("api-expose-server", "API Meta", false, "Should we include a field in the API response that tells the consumer the name of the server that served it?"))
  val apiExposeServerBuild: Switch = register(Switch("api-expose-server-build", "API Meta", false, "Should we include a field in the API response that tells the consumer the build number of the server that served it?"))
  val apiExposeCluster: Switch = register(Switch("api-expose-cluster", "API Meta", false, "Should we include a field in the API response that tells the consumer the name of the Hadoop cluster it came from?"))
  val apiExposeServedTime: Switch = register(Switch("api-expose-served-time", "API Meta", false, "Should we include a field in the API response that tells the consumer the date/time that this result was generated?"))
  val apiExposeServedTimeStamp: Switch = register(Switch("api-expose-served-timestamp", "API Meta", false, "Should we include a field in the API response that tells the consumer the timestamp that this result was generated?"))

  //API Item level Meta
  val apiExposeCPC: Switch = register(Switch("api-expose-CPC", "API Meta", false, "Should we include a field in the API response that shows the campaign CPC?"))
  val apiExposeGrossRevenue: Switch = register(Switch("api-expose-gross-revenue", "API Meta", false, "Should we include a field in the API response that shows the gross revenue?"))
  val apiExposePlacementSummary: Switch = register(Switch("api-expose-placement-summary", "API Meta", false, "Should we include a field in the API response for the placement summary?"))
  val apiExposeOrdinalArticleClickUrl: Switch = register(Switch("api-expose-ordinal-article-click-url", "API Meta", false, "Should we include a field in the API response that tells the consumer the ordinal article click url?"))

  //BEACON
  val beaconApidSyncRedirect: Variable = register(Variable("beacon-apid-sync-redirect", "Beacon", 0, "Probability (0 through 1) of using redirect argument to sync an APID back to Gravity"))

  //Live Traffic
  val liveTrafficVolume: Variable = register(Variable("live-traffic-volume", "LiveTraffic", 1000, "Number of live traffic requests to keep"))
  val liveTrafficVerbose: Switch = register(Switch("live-traffic-verbose", "LiveTraffic", false, "Send events for all sites, not just those that are enabled?"))
  
  // WF Top Impression PLacement Ids
  val preloadTopPlacements: Switch = register(Switch("preloadTopPlacements", "preloadTopPlacements", false, "Preload Top Placements"))
  val preloadTopPlacementsIds: Setting = register(Setting("preloadTopPlacementIds", "preloadTopPlacements", "", "The list of top placement ids to pre-load on startup"))
  val preloadTopPlacementsThreadCount = register(Variable("preloadTopPlacementsThreadCount", "preloadTopPlacements", 10.0, "The number of threads in preload par array"))

  //Ingestion
  val disableContentGroupIngestion: Switch = register(Switch("disable-content-group-ingestion", "Ingestion", false, "Disable Ingestion into Content Groups"))
  val ingestionMonitoringNewContentMinutes: Variable = register(Variable("ingestion-monitoring-new-content-minutes", "Ingestion", Int.MaxValue.toDouble, "RSS feed should have new articles at least every (minutes)"))
  val ingestionMonitoringDurationMinutes: Variable = register(Variable("ingestion-monitoring-duration-minutes", "Ingestion", 20, "RSS feed ingestion should not take more than (minutes)"))
  val datafeedRegraphRequests: Switch = register(Switch("datafeed-regraph-requests", "Ingestion", false, "if false, re-graph requests from datafeeds role will be NOT be honored."))

  // Content Review
  val isTrustedContent: Switch = register(Switch("is-trusted-content", "Content Review", false, "If set to true, then content added by external users will not require a review"))
  val useContentReview: Switch = register(Switch("use-content-review", "Content Review", true, "If set to true, then content will be available for review. Otherwise, no content will be returned"))

  // Algo Dimensions and Entity (fine grain control to collect metrics independently )
  val collectDimensionMetrics = register(Switch("collect-dimension-metrics", "Reco Dimension", false, "Collect metrics for this Dimension"))
  val targetDimension = register(Switch("target-dimension", "Reco Dimension", false, "Generate Recos Targetting this Dimension"))
  val fetchDimension = register(Switch("fetch-dimension", "Reco Dimension", false, "Fetch Recos Targetting this Dimension"))

  val collectEntityMetrics = register(Switch("collect-entity-metrics", "Reco Dimension", false, "Collect metrics for this Entity"))
  val targetEntity = register(Switch("target-entity", "Reco Dimension", false, "Generate Recos Targetting this Entity"))
  val fetchEntity = register(Switch("fetch-entity", "Reco Dimension", false, "Fetch Recos Targetting this Entity"))

  val useDimensions = register(Switch("useGeoRegions", "Reco Dimension", false, "Use Dimensions"))
  val DEFAULT_NO_ENTITY_VALUE = "DEFAULT_ENTITY"
  val recommendationEntityAlgoContext = register(Setting("recommendationEntityAlgoContext", "Reco Dimension", DEFAULT_NO_ENTITY_VALUE, "Only used in RecommendationAlgoContext overrides"))

  //Self Churner
  val selfChurnerUseProbationaryPeriod: Switch = register(Switch("self-churner-use-probationary-period", "Self Churner", false, "If true, the self churner will privilege new articles"))
  val selfChurnerImpressionFloor: Variable = register(Variable("self-churner-impressino-threshold", "Self Churner", 3000, "Set the impressions an article needs before it is considered high certainty"))

  val selfChurnerPeriodicBoostThreshold: Variable = register(Variable("self-churner-periodic-boost-threshold", "Self Churner", 10000, "Set the periodic impressions each article should ideally get"))
  val selfChurnerNewArticleBoostThreshold: Variable = register(Variable("self-churner-new-article-boost-threshold", "Self Churner", 10000, "Set the impressions new articles should ideally get"))


  // Scoped Metrics
  val collectUserTypeMetrics: Switch = register(Switch("collectUserTypeMetrics", "Scoped Metrics", true, "Collect User Type Scoped Metrics"))
  val collectRecommenderIdMetrics: Switch = register(Switch("collectRecommenderIdMetrics", "Scoped Metrics", true, "Collect Recommender Id Scoped Metrics"))
  val collectAlgoIdMetrics: Switch = register(Switch("collectAlgoIdMetrics", "Scoped Metrics", true, "Collect Algo ID Scoped Metrics"))
  val collectCountryMetrics: Switch = register(Switch("collectCountryMetrics", "Scoped Metrics", true, "Collect Country Metrics"))
  val collectStateMetrics: Switch = register(Switch("collectStateMetrics", "Scoped Metrics", false, "Collect State Metrics"))
  val collectDmaMetrics: Switch = register(Switch("collectDmaMetrics", "Scoped Metrics", false, "Collect DMA Metrics"))
  val collectMinuteMetrics: Switch = register(Switch("collectMinuteMetrics", "Scoped Metrics", false, "Collect Minute level Metrics"))
  val collectMetricsForHours: Variable = register(Variable("collectMetricsForHours", "Scoped Metrics", 168, "Algo will consider article metrics collected for this many hours"))
  val sendLiveMetricsUpdates: Switch = register(Switch("sendLiveMetricsUpdates", "Scoped Metrics", true, "Send Live Metrics Updates"))

  // pin user to bucket on fetch
  val pinUserToBucket: Switch = register(Switch("pinUserToBucket", "API Fetch", true, "Pin a user to a particular bucket on fetch"))

  // API Geo and Device Counters
  val collectRecommendationSiteLevelCounters: Switch = register(Switch("collectRecommendationSiteLevelCounters", "RecommendationCounters", false, "Collect Recommendation Site Level Counters"))
  val collectRecommendationGeoCounters: Switch = register(Switch("collectRecommendationGeoCounters", "RecommendationCounters", false, "Collect Recommendation Geo Counters"))
  val collectRecommendationDeviceCounters: Switch = register(Switch("collectRecommendationDeviceCounters", "RecommendationCounters", false, "Collect Recommendation Device Counters"))

  // RECOGEN1
  val upsertToS3 = register(Switch("upsertToS3", "Recogen1", false, "Upsert json document to s3 bucket"))
  val recogenThrottle: Variable = register(Variable("recogen-throttle-milliseconds", "Recogen1", 60000.0D, "Set the interval in milliseconds that must pass between recogen pass for a placement/Site."))

  // Recogen2
  val recogen2Throttle: Variable = register(Variable("recogen2-throttle-milliseconds", "Recogen2", 60000.0D, "Set the interval in milliseconds that must pass between recogen pass for a placement/Site."))
  val useLiveArticleMetrics: Switch = register(Switch("useLiveArticleMetrics", "Recogen2", false, "Use Live Metrics"))


  // Recogen2 Event
  val logRecogen2Event = register(Switch("logRecogen2Event", "Recogen2", false, "Log Recogen Event details "))
  val recogen2LogEventThrottle: Variable = register(Variable("recogen2LogEventThrottle", "Recogen2", 600000.0D, "Set the interval in milliseconds that must pass between logging recogen log events. Default 10 mins"))
  val recogen2LogEventWorksheetLimit: Variable = register(Variable("recogen2LogEventWorksheetLimit", "Recogen2", 50.0D, "Limit of the top scored articles to include in the event from the worksheet"))

  // Reco Storage fetch routing
  val useRandomRecoStorageRouteId: Switch = register(Switch("useRandomRecoStorageRouteId", "Reco Storage", false, "A switch to choose random reco storage host instead of the current roueId strategy"))

  // Test only
  val testAlgoSettingDefaultValue = -1.0D
  val testAlgoSetting: Variable = register(Variable("test-name", "Test", testAlgoSettingDefaultValue, "Test algo setting mainly for unit tests for algo setting update api."))
}
