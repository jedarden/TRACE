# Bead bf-4oav: Ad Network Macro Capture - Verification

## Task
Parse query string on page load, capture all ad network macros. Store raw in event payload alongside normalized names.

## Requirements
- **Taboola**: tb_cli_campaign, tb_click_id
- **Outbrain**: obOrigUrl, outbrain_params
- **MGID**: trid, utm_content
- **RevContent**: rc_uuid, widget_id

## Implementation Status
**VERIFIED - ALREADY COMPLETE**

The feature was previously implemented in commits:
- `3ac7009` - feat(tag): add ad network macro capture for Taboola, Outbrain, MGID, RevContent
- `4bd9912` - fix(tag): update minified tag with ad network macro capture

## Implementation Details (client/tag.js lines 128-167)

```javascript
adn: {
  // Taboola
  taboola: {
    campaign_id: urlParams.tb_cli_campaign,
    click_id: urlParams.tb_click_id,
    _raw: {
      tb_cli_campaign: urlParams.tb_cli_campaign,
      tb_click_id: urlParams.tb_click_id
    }
  },
  // Outbrain
  outbrain: {
    orig_url: urlParams.obOrigUrl,
    params: urlParams.outbrain_params,
    _raw: {
      obOrigUrl: urlParams.obOrigUrl,
      outbrain_params: urlParams.outbrain_params
    }
  },
  // MGID
  mgid: {
    trid: urlParams.trid,
    utm_content: urlParams.utm_content,
    _raw: {
      trid: urlParams.trid,
      utm_content: urlParams.utm_content
    }
  },
  // RevContent
  revcontent: {
    uuid: urlParams.rc_uuid,
    widget_id: urlParams.widget_id,
    _raw: {
      rc_uuid: urlParams.rc_uuid,
      widget_id: urlParams.widget_id
    }
  }
}
```

## Verification
- Query string parsing: Line 113 (`new URLSearchParams(window.location.search).forEach(...)`)
- All 4 ad networks captured with their specific macros
- Raw values stored in `_raw` fields alongside normalized names
- Minified version (tag.min.js) also includes the feature
