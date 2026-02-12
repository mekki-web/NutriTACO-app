package com.mekki.taco.presentation.ui.search

import androidx.lifecycle.SavedStateHandle
import com.mekki.taco.data.db.dao.FoodDao
import com.mekki.taco.data.db.entity.Food
import com.mekki.taco.utils.normalizeForSearch
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.FlowPreview
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.debounce
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.launchIn
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.flow.stateIn


data class FoodSearchState(
    val searchTerm: String = "",
    val isLoading: Boolean = false,
    val results: List<Food> = emptyList(),
    val expandedFoodId: Int? = null,
    val quickAddAmount: String = "100",
    val sortOption: FoodSortOption = FoodSortOption.RELEVANCE,
    val filterState: FoodFilterState = FoodFilterState.DEFAULT
)

private const val KEY_SM_TERM = "sm_search_term"
private const val KEY_SM_QUICK_ADD = "sm_quick_add"
private const val KEY_SM_SORT = "sm_sort_option"
private const val KEY_SM_EXPANDED = "sm_expanded_id"
private const val KEY_SM_SOURCE = "sm_source"
private const val KEY_SM_FILTER_STATE = "sm_filter_state"

/**
 * Reusable food search manager with FTS support, synonyms, and sorting.
 */
@OptIn(FlowPreview::class, ExperimentalCoroutinesApi::class)
class FoodSearchManager(
    private val foodDao: FoodDao,
    private val scope: CoroutineScope,
    private val savedStateHandle: SavedStateHandle? = null
) {
    private val _localSearchTerm = MutableStateFlow("")
    private val _localQuickAdd = MutableStateFlow("100")
    private val _localExpandedId = MutableStateFlow<Int?>(null)

    private val searchTermFlow =
        savedStateHandle?.getStateFlow(KEY_SM_TERM, "") ?: _localSearchTerm.asStateFlow()
    private val quickAddFlow =
        savedStateHandle?.getStateFlow(KEY_SM_QUICK_ADD, "100") ?: _localQuickAdd.asStateFlow()
    private val expandedIdFlow = savedStateHandle?.getStateFlow<Int?>(KEY_SM_EXPANDED, null)
        ?: _localExpandedId.asStateFlow()

    private val _localSourceFilter = MutableStateFlow(FoodSource.ALL)
    private val sourceFilterFlow = savedStateHandle?.getStateFlow(KEY_SM_SOURCE, FoodSource.ALL)
        ?: _localSourceFilter.asStateFlow()

    private val _localFilterState = MutableStateFlow(FoodFilterState.DEFAULT)
    private val filterStateFlow =
        savedStateHandle?.getStateFlow(KEY_SM_FILTER_STATE, FoodFilterState.DEFAULT)
            ?: _localFilterState.asStateFlow()

    val categories: StateFlow<List<String>> = foodDao.getAllCategories()
        .stateIn(scope, kotlinx.coroutines.flow.SharingStarted.Lazily, emptyList())

    private val _isLoading = MutableStateFlow(false)
    private val _rawResults = MutableStateFlow<List<Food>>(emptyList())

    val state: StateFlow<FoodSearchState> = combine(
        searchTermFlow,
        _isLoading,
        _rawResults,
        expandedIdFlow,
        quickAddFlow,
        filterStateFlow
    ) { term, loading, raw, expanded, quickAdd, filters ->
        val filtered = applyFilters(raw, filters)

        val sorted = when (filters.sortOption) {
            FoodSortOption.RELEVANCE -> filtered
            FoodSortOption.NAME -> filtered.sortedBy { it.name }
            // Macros
            FoodSortOption.CALORIES -> filtered.sortedByDescending { it.energiaKcal ?: 0.0 }
            FoodSortOption.PROTEIN -> filtered.sortedByDescending { it.proteina ?: 0.0 }
            FoodSortOption.CARBS -> filtered.sortedByDescending { it.carboidratos ?: 0.0 }
            FoodSortOption.FAT -> filtered.sortedByDescending { it.lipidios?.total ?: 0.0 }
            FoodSortOption.FIBER -> filtered.sortedByDescending { it.fibraAlimentar ?: 0.0 }
            FoodSortOption.CHOLESTEROL -> filtered.sortedByDescending { it.colesterol ?: 0.0 }
            // Minerals
            FoodSortOption.SODIUM -> filtered.sortedByDescending { it.sodio ?: 0.0 }
            FoodSortOption.POTASSIUM -> filtered.sortedByDescending { it.potassio ?: 0.0 }
            FoodSortOption.CALCIUM -> filtered.sortedByDescending { it.calcio ?: 0.0 }
            FoodSortOption.MAGNESIUM -> filtered.sortedByDescending { it.magnesio ?: 0.0 }
            FoodSortOption.PHOSPHORUS -> filtered.sortedByDescending { it.fosforo ?: 0.0 }
            FoodSortOption.IRON -> filtered.sortedByDescending { it.ferro ?: 0.0 }
            FoodSortOption.ZINC -> filtered.sortedByDescending { it.zinco ?: 0.0 }
            FoodSortOption.COPPER -> filtered.sortedByDescending { it.cobre ?: 0.0 }
            FoodSortOption.MANGANESE -> filtered.sortedByDescending { it.manganes ?: 0.0 }
            // Vitamins
            FoodSortOption.VITAMIN_C -> filtered.sortedByDescending { it.vitaminaC ?: 0.0 }
            FoodSortOption.RETINOL -> filtered.sortedByDescending { it.retinol ?: 0.0 }
            FoodSortOption.THIAMINE -> filtered.sortedByDescending { it.tiamina ?: 0.0 }
            FoodSortOption.RIBOFLAVIN -> filtered.sortedByDescending { it.riboflavina ?: 0.0 }
            FoodSortOption.PYRIDOXINE -> filtered.sortedByDescending { it.piridoxina ?: 0.0 }
            FoodSortOption.NIACIN -> filtered.sortedByDescending { it.niacina ?: 0.0 }
        }
        FoodSearchState(
            searchTerm = term,
            isLoading = loading,
            results = sorted,
            expandedFoodId = expanded,
            quickAddAmount = quickAdd,
            sortOption = filters.sortOption,
            filterState = filters
        )
    }
        .distinctUntilChanged()
        .stateIn(scope, kotlinx.coroutines.flow.SharingStarted.Lazily, FoodSearchState())

    private val stopWords = setOf(
        "de", "com", "da", "do", "para", "em", "um", "uma", "a", "o", "as", "os"
    )

    private val rawSynonyms = mapOf(
        "frango" to "galinha",
        "boi" to "bovino",
        "carne" to "bovino",
        "pao" to "padaria",
        "mandioca" to "aipim",
        "porco" to "suino",
        "batata" to "inglesa"
    )

    private val synonyms: Map<String, Set<String>> by lazy {
        val map = mutableMapOf<String, MutableSet<String>>()
        rawSynonyms.forEach { (key, value) ->
            map.getOrPut(key) { mutableSetOf() }.add(value)
            map.getOrPut(value) { mutableSetOf() }.add(key)
        }
        map
    }

    init {
        observeSearchTerm()
    }

    private fun observeSearchTerm() {
        combine(searchTermFlow, filterStateFlow) { term, filters -> Pair(term, filters) }
            .debounce(300)
            .distinctUntilChanged()
            .flatMapLatest { (term, filters) ->
                val hasCategories = filters.selectedCategories.isNotEmpty()
                val hasAdvanced = filters.hasAdvancedFilters
                val hasSourceFilter = filters.source != FoodSource.ALL
                val hasAnyFilter = hasCategories || hasAdvanced || hasSourceFilter

                if (term.length < 2 && !hasAnyFilter) {
                    _isLoading.value = false
                    flowOf(emptyList())
                } else if (hasAnyFilter && term.length < 2) {
                    _isLoading.value = true
                    if (hasCategories) {
                        foodDao.getFoodsByCategories(filters.selectedCategories.toList())
                    } else {
                        foodDao.getAllFoods()
                    }
                } else {
                    _isLoading.value = true
                    val ftsQuery = buildFtsString(term)
                    if (ftsQuery == null) {
                        _isLoading.value = false
                        flowOf(emptyList())
                    } else {
                        val normalized = term.normalizeForSearch()
                        foodDao.searchFoodsInternal(normalized, ftsQuery)
                    }
                }
            }
            .onEach { results ->
                _rawResults.value = results
                _isLoading.value = false
            }
            .launchIn(scope)
    }

    private fun buildFtsString(input: String): String? {
        val normalized = input.normalizeForSearch()
        val tokens = normalized.split(" ").filter { it.isNotBlank() && it !in stopWords }

        if (tokens.isEmpty()) return null

        val queryParts = tokens.map { token ->
            val forms = mutableSetOf<String>()

            // Improved Portuguese plural handling
            val stem = when {
                // Words ending in "ões" -> singular is "ão" (ex: limões -> limão)
                token.length > 4 && token.endsWith("oes") -> token.dropLast(3) + "ao"
                // Words ending in "ães" -> singular is "ão" (ex: pães -> pão)
                token.length > 4 && token.endsWith("aes") -> token.dropLast(3) + "ao"
                // Words ending in "es" with length > 4 -> try removing "es" (ex: tomates -> tomat)
                token.length > 4 && token.endsWith("es") -> token.dropLast(2)
                // Words ending in "s" with length > 4 -> try removing "s" (ex: bananas -> banana)
                // But avoid words where "s" is part of the root (like "paus")
                token.length > 4 && token.endsWith("s") && !token.endsWith("us") -> token.dropLast(1)
                else -> token
            }

            forms.add(token)
            if (stem != token) {
                forms.add(stem)
            }

            synonyms[token]?.let { forms.addAll(it) }
            synonyms[stem]?.let { forms.addAll(it) }

            if (forms.size > 1) {
                "(" + forms.joinToString(" OR ") { "$it*" } + ")"
            } else {
                "${forms.first()}*"
            }
        }

        return queryParts.joinToString(" ")
    }

    fun onSearchTermChange(term: String) {
        if (savedStateHandle != null) {
            savedStateHandle[KEY_SM_TERM] = term
            savedStateHandle[KEY_SM_EXPANDED] = null
        } else {
            _localSearchTerm.value = term
            _localExpandedId.value = null
        }
    }

    fun onSortOptionChange(option: FoodSortOption) {
        updateFilterState(filterStateFlow.value.copy(sortOption = option))
    }

    fun onFoodToggled(foodId: Int) {
        if (savedStateHandle != null) {
            val current = savedStateHandle.get<Int?>(KEY_SM_EXPANDED)
            if (current == foodId) {
                savedStateHandle[KEY_SM_EXPANDED] = null
            } else {
                savedStateHandle[KEY_SM_EXPANDED] = foodId
                savedStateHandle[KEY_SM_QUICK_ADD] = "100"
            }
        } else {
            val current = _localExpandedId.value
            if (current == foodId) {
                _localExpandedId.value = null
            } else {
                _localExpandedId.value = foodId
                _localQuickAdd.value = "100"
            }
        }
    }

    fun onQuickAddAmountChange(amount: String) {
        val filtered = amount.filter { it.isDigit() || it == '.' }
        if (savedStateHandle != null) {
            savedStateHandle[KEY_SM_QUICK_ADD] = filtered
        } else {
            _localQuickAdd.value = filtered
        }
    }

    fun clear() {
        if (savedStateHandle != null) {
            savedStateHandle[KEY_SM_TERM] = ""
            savedStateHandle[KEY_SM_EXPANDED] = null
            savedStateHandle[KEY_SM_QUICK_ADD] = "100"
            savedStateHandle[KEY_SM_FILTER_STATE] = FoodFilterState.DEFAULT
        } else {
            _localSearchTerm.value = ""
            _localExpandedId.value = null
            _localQuickAdd.value = "100"
            _localFilterState.value = FoodFilterState.DEFAULT
        }
        _rawResults.value = emptyList()
    }

    fun restoreState(
        searchTerm: String,
        filterState: FoodFilterState,
        expandedId: Int?,
        quickAddAmount: String
    ) {
        if (savedStateHandle != null) {
            savedStateHandle[KEY_SM_TERM] = searchTerm
            savedStateHandle[KEY_SM_EXPANDED] = expandedId
            savedStateHandle[KEY_SM_QUICK_ADD] = quickAddAmount
            savedStateHandle[KEY_SM_FILTER_STATE] = filterState
        } else {
            _localSearchTerm.value = searchTerm
            _localExpandedId.value = expandedId
            _localQuickAdd.value = quickAddAmount
            _localFilterState.value = filterState
        }
    }

    fun onFilterStateChange(newState: FoodFilterState) {
        updateFilterState(newState)
    }

    fun onSourceFilterChange(source: FoodSource) {
        updateFilterState(filterStateFlow.value.copy(source = source))
    }

    fun onCategoryToggle(category: String) {
        val current = filterStateFlow.value.selectedCategories
        val updated = if (category in current) current - category else current + category
        updateFilterState(filterStateFlow.value.copy(selectedCategories = updated))
    }

    fun onClearCategories() {
        updateFilterState(filterStateFlow.value.copy(selectedCategories = emptySet()))
    }

    private fun updateFilterState(newState: FoodFilterState) {
        if (savedStateHandle != null) {
            savedStateHandle[KEY_SM_FILTER_STATE] = newState
        } else {
            _localFilterState.value = newState
        }
    }

    // Helper function to check if a nutrient value is within a range
    private fun checkRange(value: Double?, min: Double?, max: Double?): Boolean {
        val v = value ?: 0.0
        return (min == null || v >= min) && (max == null || v <= max)
    }

    private fun applyFilters(foods: List<Food>, filters: FoodFilterState): List<Food> {
        // Early return if no filters are active
        if (!filters.hasAdvancedFilters && filters.source == FoodSource.ALL && filters.selectedCategories.isEmpty()) {
            return foods
        }

        return foods.filter { food ->
            // Check source filter
            val sourceMatch = when (filters.source) {
                FoodSource.ALL -> true
                FoodSource.TACO -> !food.isCustom
                FoodSource.CUSTOM -> food.isCustom
            }
            if (!sourceMatch) return@filter false

            // Check category filter
            if (filters.selectedCategories.isNotEmpty() && food.category !in filters.selectedCategories) {
                return@filter false
            }

            // Macronutrients
            if (!checkRange(food.proteina, filters.minProtein, filters.maxProtein)) return@filter false
            if (!checkRange(food.carboidratos, filters.minCarbs, filters.maxCarbs)) return@filter false
            if (!checkRange(food.lipidios?.total, filters.minFat, filters.maxFat)) return@filter false
            if (!checkRange(food.energiaKcal, filters.minCalories, filters.maxCalories)) return@filter false
            if (!checkRange(food.fibraAlimentar, filters.minFibra, filters.maxFibra)) return@filter false
            if (!checkRange(food.colesterol, filters.minColesterol, filters.maxColesterol)) return@filter false

            // Lipid subtypes
            if (!checkRange(food.lipidios?.saturados, filters.minSaturados, filters.maxSaturados)) return@filter false
            if (!checkRange(food.lipidios?.monoinsaturados, filters.minMonoinsaturados, filters.maxMonoinsaturados)) return@filter false
            if (!checkRange(food.lipidios?.poliinsaturados, filters.minPoliinsaturados, filters.maxPoliinsaturados)) return@filter false

            // Vitamins
            if (!checkRange(food.vitaminaC, filters.minVitaminaC, filters.maxVitaminaC)) return@filter false
            if (!checkRange(food.retinol, filters.minRetinol, filters.maxRetinol)) return@filter false
            if (!checkRange(food.tiamina, filters.minTiamina, filters.maxTiamina)) return@filter false
            if (!checkRange(food.riboflavina, filters.minRiboflavina, filters.maxRiboflavina)) return@filter false
            if (!checkRange(food.piridoxina, filters.minPiridoxina, filters.maxPiridoxina)) return@filter false
            if (!checkRange(food.niacina, filters.minNiacina, filters.maxNiacina)) return@filter false

            // Minerals
            if (!checkRange(food.calcio, filters.minCalcio, filters.maxCalcio)) return@filter false
            if (!checkRange(food.ferro, filters.minFerro, filters.maxFerro)) return@filter false
            if (!checkRange(food.sodio, filters.minSodio, filters.maxSodio)) return@filter false
            if (!checkRange(food.potassio, filters.minPotassio, filters.maxPotassio)) return@filter false
            if (!checkRange(food.magnesio, filters.minMagnesio, filters.maxMagnesio)) return@filter false
            if (!checkRange(food.zinco, filters.minZinco, filters.maxZinco)) return@filter false
            if (!checkRange(food.cobre, filters.minCobre, filters.maxCobre)) return@filter false
            if (!checkRange(food.fosforo, filters.minFosforo, filters.maxFosforo)) return@filter false
            if (!checkRange(food.manganes, filters.minManganes, filters.maxManganes)) return@filter false

            // Other
            if (!checkRange(food.umidade, filters.minUmidade, filters.maxUmidade)) return@filter false
            if (!checkRange(food.cinzas, filters.minCinzas, filters.maxCinzas)) return@filter false

            // Amino acids (most foods don't have this data, so check if filter is active first)
            if (filters.hasAminoAcidFilters) {
                val aa = food.aminoacidos
                if (!checkRange(aa?.triptofano, filters.minTriptofano, filters.maxTriptofano)) return@filter false
                if (!checkRange(aa?.treonina, filters.minTreonina, filters.maxTreonina)) return@filter false
                if (!checkRange(aa?.isoleucina, filters.minIsoleucina, filters.maxIsoleucina)) return@filter false
                if (!checkRange(aa?.leucina, filters.minLeucina, filters.maxLeucina)) return@filter false
                if (!checkRange(aa?.lisina, filters.minLisina, filters.maxLisina)) return@filter false
                if (!checkRange(aa?.metionina, filters.minMetionina, filters.maxMetionina)) return@filter false
                if (!checkRange(aa?.cistina, filters.minCistina, filters.maxCistina)) return@filter false
                if (!checkRange(aa?.fenilalanina, filters.minFenilalanina, filters.maxFenilalanina)) return@filter false
                if (!checkRange(aa?.tirosina, filters.minTirosina, filters.maxTirosina)) return@filter false
                if (!checkRange(aa?.valina, filters.minValina, filters.maxValina)) return@filter false
                if (!checkRange(aa?.arginina, filters.minArginina, filters.maxArginina)) return@filter false
                if (!checkRange(aa?.histidina, filters.minHistidina, filters.maxHistidina)) return@filter false
                if (!checkRange(aa?.alanina, filters.minAlanina, filters.maxAlanina)) return@filter false
                if (!checkRange(aa?.acidoAspartico, filters.minAcidoAspartico, filters.maxAcidoAspartico)) return@filter false
                if (!checkRange(aa?.acidoGlutamico, filters.minAcidoGlutamico, filters.maxAcidoGlutamico)) return@filter false
                if (!checkRange(aa?.glicina, filters.minGlicina, filters.maxGlicina)) return@filter false
                if (!checkRange(aa?.prolina, filters.minProlina, filters.maxProlina)) return@filter false
                if (!checkRange(aa?.serina, filters.minSerina, filters.maxSerina)) return@filter false
            }

            true
        }
    }
}