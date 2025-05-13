# Prompt Template Examples

This document provides examples of how to customize the prompt templates in SwellForecaster.

## Basic Template Customization

Here are practical examples of how to customize the prompt templates to change the forecast output.

### Changing the Forecaster Personality

**Original:**
```json
"intro": "You are a veteran Hawaiian surf forecaster with over 30 years of experience analyzing Pacific storm systems and delivering detailed, educational surf forecasts for Hawaii."
```

**Modified (More Technical):**
```json
"intro": "You are a PhD oceanographer specializing in wave dynamics and Pacific swell patterns. Your forecasts are highly technical, data-driven, and emphasize the meteorological and oceanographic principles behind wave generation and propagation."
```

**Modified (More Casual):**
```json
"intro": "You are a lifelong Hawaiian surfer with decades in the water and a knack for reading ocean conditions. Your forecasts are conversational, practical, and focused on what the average surfer needs to know to score the best waves."
```

### Emphasizing a Specific Break

If you want to focus on a specific surf break, you can add a specialized section:

```json
"specialized": {
  "pipeline_focus": "Pipeline-Specific Analysis:\n- Pay special attention to swell direction and period for Pipeline\n- Note that Pipeline works best on NW swells (305-315°) with 12-18s periods\n- Be specific about size thresholds: under 4ft won't break properly, over 12ft becomes extremely dangerous\n- Mention whether Second and Third Reef will be breaking\n- Note tidal effects on wave quality (generally better on incoming/mid tide)\n- Include crowd factor predictions based on conditions"
}
```

Then modify `pacific_forecast_analyzer.py` to use this template when appropriate:

```python
# Add Pipeline-specific analysis if we're in winter season and significant NW swell
if current_month in [11, 12, 1, 2, 3] and significant_nw_swell:
    prompt += "\n\n" + PROMPTS["forecast"]["specialized"]["pipeline_focus"]
```

### Adding a New Template Section for Windsurfers

Add a new section to `prompts.json`:

```json
"windsurfing": {
  "intro": "WINDSURFING FORECAST:\n- Analyze wind patterns specifically for windsurfing (15-30 knots ideal)\n- Note daily time windows with best wind (e.g., 'Thursday 1-5PM: ENE 25 knots, excellent at Diamond Head')\n- Include specific locations with appropriate wind and wave conditions\n- Consider chop and wave state as they affect sailing conditions\n- Note any hazards like strong currents or shallow reef areas to avoid"
}
```

Then add code to use this template when appropriate:

```python
# Add windsurfing forecast if enabled in config
if cfg["FORECAST"].getboolean("include_windsurfing", False):
    prompt += "\n\n" + PROMPTS["forecast"]["windsurfing"]["intro"]
```

## Template Variable Examples

Here's how template variables are formatted and replaced at runtime:

**Template:**
```json
"data_sources": "AVAILABLE DATA SOURCES:\n1. NDBC Buoy Readings: {buoy_summary}\n2. NOAA CO-OPS Wind Observations: {wind_summary}\n3. Southern Hemisphere Data: {southern_summary}\n4. North Shore Analysis: {north_shore_summary}\n5. ECMWF Wave Model Data: {ecmwf_summary}\n6. Australian BOM Data: {bom_summary}\n7. WW3 Wave Model Data: {model_summary}"
```

**Code to format with actual data:**
```python
# Example of how variables are replaced at runtime
if PROMPTS and "forecast" in PROMPTS:
    data_sources_text = PROMPTS["forecast"]["data_sources"].format(
        buoy_summary="Buoys 51001 and 51101 showing NW swell at 3.5ft @ 12s",
        wind_summary="ENE trades 15-20 knots, stronger in channels",
        southern_summary="SSW swell expected to arrive Wednesday",
        north_shore_summary="Multi-phase NW swell from storm near Japan",
        ecmwf_summary="Shows consistent NW energy for next 7 days",
        bom_summary="Low pressure system forming near New Zealand",
        model_summary="WW3 shows 5ft @ 14s from 320° arriving Saturday"
    )
```

## Advanced: Conditional Templates

You can use different templates based on conditions. For example, different seasonal templates:

```json
"seasonal": {
  "winter": "North Shore season is in full effect. Focus on analyzing North Pacific storm systems and their impact on North Shore breaks.",
  "summer": "South Shore season is active now. Concentrate on Southern Hemisphere storm systems and their development near New Zealand and Antarctica."
}
```

**Code to use seasonal templates:**
```python
# Determine which seasonal template to use based on current month
current_month = datetime.now().month
if 4 <= current_month <= 9:  # April through September
    season = "summer"
else:  # October through March
    season = "winter"

# Add the appropriate seasonal emphasis
if PROMPTS and "seasonal" in PROMPTS:
    prompt += "\n\n" + PROMPTS["seasonal"][season]
```

These examples demonstrate how to extend and customize the prompt templating system to adapt the forecast generation to your specific needs.