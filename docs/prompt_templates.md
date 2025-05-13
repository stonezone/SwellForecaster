# Prompt Template Guide

This document provides a comprehensive guide to SwellForecaster's prompt templating system, which allows for customization of AI-generated forecasts without modifying code.

## Overview

The prompt templating system uses a JSON configuration file (`prompts.json`) that stores structured templates for different aspects of the forecasting process. These templates use string interpolation with `{variable}` syntax to inject dynamic data at runtime.

## File Location

The default prompt configuration is stored at:
```
/path/to/SwellForecaster/prompts.json
```

## Structure

The `prompts.json` file is organized into the following main sections:

### 1. `forecast`

Contains templates related to the main forecast generation:

- **`intro`**: The primary system instruction for the AI forecaster
- **`emphasis`**: Configurable emphasis sections for different conditions
- **`data_sources`**: Template for listing available data sources
- **`structure`**: Templates for different forecast structural components
- **`specialized`**: Domain-specific analysis templates

### 2. `chart_generation`

Contains templates for generating visual charts and graphics.

## Template Variables

Templates can include variables that get replaced at runtime with actual data. Variables use the format `{variable_name}`. Common variables include:

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `{timestamp}` | When data was collected | "May 9, 2023 at 08:00 UTC" |
| `{buoy_summary}` | Summary of available buoy data | "Buoys 51001, 51101, 51000, and 51003 showing NW swell at 5.2ft @ 14s" |
| `{wind_summary}` | Summary of wind conditions | "Trade winds 15-20kts from ENE" |
| `{north_shore_summary}` | North Shore analysis | "Multi-phase NW swell arriving Thursday" |
| `{southern_summary}` | Southern Hemisphere analysis | "SSW swell building to 4-6ft by weekend" |
| `{model_summary}` | Wave model data summary | "WW3 shows rising NW energy" |
| `{caldwell_analysis}` | Pat Caldwell's official analysis | "South swell to peak Wednesday..." |
| `{swell_details}` | Specific swell information | "SSW 3.2ft @ 16s arriving Saturday" |

## Example Usage

### Basic Template Modification

To modify how the AI forecaster introduces itself:

1. Open `prompts.json`
2. Locate the `forecast.intro` section
3. Edit the template:

```json
"intro": "You are a veteran Hawaiian surf forecaster with over 30 years of experience analyzing Pacific storm systems and delivering detailed, educational surf forecasts for Hawaii.\n\nUse your deep expertise in swell mechanics, Pacific climatology, and historical analogs to analyze the following marine data (surf, wind, swell) collected {timestamp} and generate a 10-day surf forecast for Oʻahu."
```

### Emphasizing a Specific Region

To emphasize either North Shore or South Shore conditions:

```json
"emphasis": {
  "south": "IMPORTANT: Currently there is significant South Pacific storm activity generating large south swells. Your South Shore forecast section should receive extra attention and detail in this report.",
  "north": "IMPORTANT: Current North Pacific data shows a multi-phase storm system with distinct components. Your North Shore forecast should break down this system into its separate phases (similar to Pat Caldwell's approach) and track how each phase affects swell arrival timing and characteristics."
}
```

### Changing Forecast Structure

To modify how the forecast is structured:

```json
"structure": {
  "intro": "Structure the forecast in this style:\n1. Start with a short title and opening paragraph summarizing current surf conditions and the broader meteorological setup.",
  "nowcast": "2. Include a detailed NOWCAST section that:\n   - Uses the most recent buoy readings to describe what's happening RIGHT NOW\n   - Tracks swell propagation (e.g., '2.8 ft @ 15s from 310° hit Buoy 51001 at 8AM, arriving at North Shore around 11AM')\n   - Translates buoy data to actual surf heights at specific breaks (e.g., 'translating to 5-7 ft faces at exposed spots')\n   - Notes whether swell is building, holding, or dropping based on buoy trends"
}
```

## How Templates are Used in Code

The application loads prompt templates from `prompts.json` at runtime. If the file is missing or invalid, it falls back to hardcoded defaults.

In `pacific_forecast_analyzer.py`:

```python
def load_prompts(prompts_file="prompts.json"):
    """Load prompt templates from the JSON file"""
    try:
        with open(prompts_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        log.warning(f"Failed to load prompts from {prompts_file}: {e}")
        log.warning("Using default hardcoded prompts")
        return None
```

Templates are then formatted with actual data:

```python
# Example (simplified)
prompt_templates = load_prompts()
if prompt_templates and "forecast" in prompt_templates:
    intro = prompt_templates["forecast"]["intro"].format(timestamp=timestamp)
else:
    # Fallback to hardcoded prompt
```

## Best Practices

1. **Maintain Variable Names**: Keep existing variable names when editing templates to ensure proper data insertion
2. **Use Markdown**: Prompts support markdown formatting for better readability
3. **Test Changes**: After modifying prompts, run a test forecast to verify the changes work correctly
4. **Back Up Templates**: Keep a backup of your custom templates before making significant changes
5. **Document Customizations**: Keep notes on any customizations you make for future reference

## Troubleshooting

- **Missing Variables**: If a template contains a variable like `{variable_name}` that doesn't exist in the code, it will remain as-is in the final prompt
- **JSON Syntax Errors**: If the JSON file has syntax errors, the application will fall back to hardcoded defaults
- **Formatting Issues**: Use valid JSON syntax, especially for quotes and escape characters

## Advanced Customization

For more advanced prompt engineering, you can:

1. Add new variables by modifying the code in `pacific_forecast_analyzer.py`
2. Create conditional prompts based on detected data patterns
3. Create specialized prompts for specific weather conditions or seasons