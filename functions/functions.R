# Info boxes -------------------------------------------------------------
infoBox_border <- function(header = "Header here",
                           text = "More text here",
                           backgroundColour = "#ccdff1",
                           borderColour = "#005EB8",
                           width = "31%",
                           fontColour = "black") {
  #set handling for when header is blank
  display <- "block"

  if (header == "") {
    display <- "none"
  }

  paste(
    "<div class='infobox_border' style = 'border: 1px solid ",
    borderColour,
    "!important;
  border-left: 5px solid ",
  borderColour,
  "!important;
  background-color: ",
  backgroundColour,
  "!important;
  padding: 10px;
  width: ",
  width,
  "!important;
  display: inline-block;
  vertical-align: top;
  flex: 1;
  height: 100%;'>
  <h4 style = 'color: ",
  fontColour,
  ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
  display,
  ";'>",
  header,
  "</h4>
  <p style = 'color: ",
  fontColour,
  ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
  text,
  "</p>
</div>"
  )
}

infoBox_no_border <- function(header = "Header here",
                              text = "More text here",
                              backgroundColour = "#005EB8",
                              width = "31%",
                              fontColour = "white") {
  #set handling for when header is blank
  display <- "block"

  if (header == "") {
    display <- "none"
  }

  paste(
    "<div class='infobox_no_border',
    style = 'background-color: ",
    backgroundColour,
    "!important;padding: 10px;
    width: ",
    width,
    ";
    display: inline-block;
    vertical-align: top;
    flex: 1;
    height: 100%;'>
  <h4 style = 'color: ",
  fontColour,
  ";
  font-weight: bold;
  font-size: 18px;
  margin-top: 0px;
  margin-bottom: 10px;
  display: ",
  display,
  ";'>",
  header,
  "</h4>
  <p style = 'color: ",
  fontColour,
  ";
  font-size: 16px;
  margin-top: 0px;
  margin-bottom: 0px;'>",
  text,
  "</p>
</div>"
  )
}
### CSV Download button
get_download_button <- function(data = data, title = "Download chart data", filename = "data") {
  dt <- datatable(data, rownames = FALSE,
                  extensions = 'Buttons',
                  options = list(
                    searching = FALSE,
                    paging = TRUE,
                    bInfo = FALSE,
                    pageLength = 1,
                    dom = '<"datatable-wrapper"B>',
                    buttons = list(
                      list(extend = 'csv',
                           text = title,
                           filename = filename,
                           className = "nhs-button-style")
                    ),
                    initComplete = JS(
                      "function(settings, json) {",
                      "$(this.api().table().node()).css('visibility', 'collapse');",
                      "}"
                    )
                  )
  )

  return(dt)
}
#---------------


#' @title Easy helper for group chart in highcharter - amended to turn off rounding on data labels for
#'
#' @name group_chart_hc_a
#'
#' @description
#' Create group chart in highcharter
#'
#' @param data the data object to pass to the chart
#' @param x the column in the data to show on the x axis
#' @param y the column in the data to show on the y axis
#' @param type the highcharter chart type to use (defaults to line)
#' @param group the column in the data which includes the groups to use in the chart
#' @param xLab the label to use on the x axis (defaults to NULL)
#' @param yLab the label to use on the y axis (defaults to NULL)
#' @param title the chart title (defaults to NULL)
#' @param dlOn TRUE/FALSE whether to include data labels on the chart (defaults to TRUE)
#' @param currency TRUE/FALSE whether chart y values are currency (GBP) (defaults to TRUE)
#' @param marker TRUE/FALSE enable markers in chart (defaults to TRUE)
#'
#' @importFrom rlang enexpr
#' @importFrom highcharter highchart
#' @importFrom highcharter hc_chart
#' @importFrom highcharter hc_colors
#' @importFrom highcharter hc_add_series
#' @importFrom highcharter hcaes
#' @importFrom highcharter hc_xAxis
#' @importFrom highcharter hc_yAxis
#' @importFrom highcharter hc_title
#' @importFrom highcharter hc_legend
#' @importFrom highcharter hc_tooltip
#' @importFrom highcharter hc_credits
#'
#' @export
#'
#' @examples
#' group_chart_hc(data = iris,
#' x = "Sepal.Length",
#' y = "Petal.Width",
#' group = "Species",
#' type = "scatter")

group_chart_hc_unround <- function(data,
                           x,
                           y,
                           type = "line",
                           group,
                           xLab = NULL,
                           yLab = NULL,
                           title = NULL,
                           dlOn = TRUE,
                           currency = FALSE,
                           marker = TRUE) {
  # this function creates a group bar chart with NHSBSA data vis standards
  # applied. includes datalabel formatter to include "£" if needed.

  x <- rlang::enexpr(x)
  y <- rlang::enexpr(y)

  group <- rlang::enexpr(group)

  # set font to arial
  font <- "Arial"

  # get number of groups. max number of groups is 9 for unique colors
  num_groups <- length(unique(data[[group]]))

  # define a set of colors
  colors <- c("#005eb8", "#ed8b00", "#009639", "#8a1538", "#00a499")

  # if there are more groups than colors, recycle the colors
  if (num_groups > length(colors)) {
    colors <- rep(colors, length.out = num_groups)
  }


  #if there is a 'Total' groups ensure this takes the color black
  if ("Total" %in% unique(data[[group]])) {
    #identify index of "total" group
    total_index <- which(sort(unique(data[[group]])) == "Total")

    # add black to location of total_index
    colors <-
      c(colors[1:total_index - 1], "#000000", colors[total_index:length(colors)])
  }

  # subset the colors to the number of groups
  #colors <- ifelse(unique(data[[group]]) == "Total", "black", colors[1:num_groups])

  # check currency argument to set symbol
  # dlFormatter <- highcharter::JS(
  #   paste0(
  #     "function() {
  #   var ynum = this.point.y;
  #   var options = { maximumSignificantDigits: 3, minimumSignificantDigits: 3 };
  #     if (",
  #   tolower(as.character(currency)),
  #   ") {
  #     options.style = 'currency';
  #     options.currency = 'GBP';
  #     }
  #     if (ynum >= 1000000000) {
  #       options.maximumSignificantDigits = 4;
  #       options.minimumSignificantDigits = 4;
  #     }else {
  #      options.maximumSignificantDigits = 3;
  #       options.minimumSignificantDigits = 3;
  #     }
  #   return ynum.toLocaleString('en-GB', options);
  # }"
  #   )
  # )


  # ifelse(is.na(str_extract(!!y, "(?<=\\().*(?=,)")),!!y,str_extract(!!y, "(?<=\\().*(?=,)")),

  # check chart type to set grid lines
  gridlineColor <- if (type == "line")
    "#e6e6e6"
  else
    "transparent"

  # check chart type to turn on y axis labels
  yLabels <- if (type == "line")
    TRUE
  else
    FALSE

  # highchart creation
  chart <- highcharter::highchart() |>
    highcharter::hc_chart(style = list(fontFamily = font)) |>
    highcharter::hc_colors(colors) |>
    # add only series
    highcharter::hc_add_series(
      data = data,
      type = type,
      marker = list(enabled = marker),
      highcharter::hcaes(
        x = !!x,
        y = !!y,
        group = !!group
      ),
      groupPadding = 0.1,
      pointPadding = 0.05,
      dataLabels = list(
        enabled = dlOn,
        # formatter = dlFormatter,
        style = list(textOutline = "none")
      )
    ) |>
    highcharter::hc_xAxis(type = "category",
                          title = list(text = xLab)) |>
    # turn off y axis and grid lines
    highcharter::hc_yAxis(
      title = list(text = yLab),
      labels = list(enabled = yLabels),
      gridLineColor = gridlineColor,
      min = 0
    ) |>
    highcharter::hc_title(text = title,
                          style = list(fontSize = "16px",
                                       fontWeight = "bold")) |>
    highcharter::hc_legend(enabled = TRUE) |>
    highcharter::hc_tooltip(enabled = FALSE) |>
    highcharter::hc_credits(enabled = TRUE)

  # explicit return
  return(chart)
}
