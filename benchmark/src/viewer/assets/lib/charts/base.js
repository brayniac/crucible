import {
    createAxisLabelFormatter,
} from './util/units.js';
import { formatDateTime } from './util/utils.js';

/**
 * Creates a placeholder option for charts with no data
 * @param {string} title - The title of the chart
 * @returns {Object} ECharts option object for no-data placeholder
 */
export function getNoDataOption(title) {
    return {
        title: {
            text: title,
            left: 10,
            top: 8,
            textStyle: {
                color: '#c9d1d9',
                fontSize: 14,
                fontWeight: 500,
            },
        },
        graphic: {
            type: 'text',
            left: 'center',
            top: 'middle',
            style: {
                text: 'No data',
                fontSize: 13,
                fill: '#6e7681',
                font: 'normal 13px -apple-system, BlinkMacSystemFont, sans-serif',
            },
        },
        xAxis: {
            show: false,
        },
        yAxis: {
            show: false,
        },
        grid: {
            left: '12%',
            right: '3%',
            top: '40',
            bottom: '35',
        },
    };
}

/**
 * Approximates echarts' built-in tooltip formatter, but with our own x axis formatting
 * (using formatDateTime) and our own value formatting (using valueFormatter).
 * @param {function} valueFormatter - A function from raw value to formatted value.
 */
export function getTooltipFormatter(valueFormatter) {
    return (paramsArray) => {
        // Sort the params array alphabetically by series name
        const sortedParams = [...paramsArray].sort((a, b) => {
            const aName = a.seriesName;
            const bName = b.seriesName;

            const aHasId = aName.startsWith('id=');
            const bHasId = bName.startsWith('id=');

            if (aHasId && bHasId) {
                return aName.localeCompare(bName, undefined, { numeric: true });
            } else if (aHasId) {
                return -1;
            } else if (bHasId) {
                return 1;
            } else {
                return aName.localeCompare(bName, undefined, { numeric: true });
            }
        });

        const result =
            `<div>
                <div>
                    ${formatDateTime(paramsArray[0].value[0])}
                </div>
                <div style="margin-top: 5px;">
                    ${sortedParams.map(p => `<div>
                        ${p.marker}
                        <span style="margin-left: 2px;">
                            ${p.seriesName}
                        </span>
                        <span style="float: right; margin-left: 20px; font-weight: bold;">
                            ${valueFormatter(p.value[1])}
                        </span>
                    </div>`).join('')}
                </div>
            </div>`;

        return result;
    }
}

export function getBaseOption(title, interval = null) {
    // Calculate minimum zoom span (5x sampling interval in milliseconds)
    // If interval is provided in seconds, convert to ms
    const minValueSpan = interval ? interval * 5 * 1000 : undefined;

    return {
        grid: {
            left: '12%',
            right: '3%',
            top: '40',
            bottom: '35',
            containLabel: false,
        },
        xAxis: {
            type: 'time',
            min: 'dataMin',
            max: 'dataMax',
            splitNumber: 4,
            axisLine: {
                lineStyle: {
                    color: '#30363d'
                }
            },
            axisLabel: {
                color: '#8b949e',
                formatter: {
                    year: '{yyyy}',
                    month: '{MMM}',
                    day: '{d}',
                    hour: '{HH}:{mm}',
                    minute: '{HH}:{mm}',
                    second: '{HH}:{mm}:{ss}',
                    millisecond: '{hh}:{mm}:{ss}.{SSS}',
                    none: '{hh}:{mm}:{ss}.{SSS}'
                }
            },
        },
        tooltip: {
            trigger: 'axis',
            axisPointer: {
                type: 'line',
                snap: true,
                animation: false,
                label: {
                    backgroundColor: '#21262d'
                }
            },
            textStyle: {
                color: '#c9d1d9'
            },
            backgroundColor: 'rgba(22, 27, 34, 0.95)',
            borderColor: '#30363d',
        },
        // Invisible toolbox for drag-to-zoom
        toolbox: {
            orient: 'vertical',
            itemSize: 13,
            top: 15,
            right: -6,
            feature: {
                dataZoom: {
                    yAxisIndex: 'none',
                    icon: {
                        zoom: 'path://',
                        back: 'path://',
                    },
                },
            },
        },
        // Hidden dataZoom component for programmatic zoom control
        dataZoom: [{
            type: 'inside',
            xAxisIndex: 0,
            filterMode: 'none',
            zoomOnMouseWheel: true,
            moveOnMouseMove: false,
            moveOnMouseWheel: false,
            minValueSpan: minValueSpan,
        }],
        title: {
            text: title,
            left: 10,
            top: 8,
            textStyle: {
                color: '#c9d1d9',
                fontSize: 14,
                fontWeight: 500
            }
        },
        textStyle: {
            color: '#c9d1d9'
        },
        darkMode: true,
        backgroundColor: 'transparent'
    };
}

export function getBaseYAxisOption(logScale, minValue, maxValue, unitSystem) {
    return {
        type: logScale ? 'log' : 'value',
        logBase: 10,
        scale: true,
        min: minValue,
        max: maxValue,
        axisLine: {
            lineStyle: {
                color: '#30363d'
            }
        },
        axisLabel: {
            color: '#8b949e',
            margin: 12,
            formatter: unitSystem ?
                createAxisLabelFormatter(unitSystem) :
                function (value) {
                    if (logScale && Math.abs(value) >= 1000) {
                        return value.toExponential(0);
                    }
                    if (Math.abs(value) > 10000 || (Math.abs(value) > 0 && Math.abs(value) < 0.01)) {
                        return value.toExponential(1);
                    }
                    return value;
                }
        },
        splitLine: {
            lineStyle: {
                color: 'rgba(48, 54, 61, 0.6)'
            }
        }
    };
}
