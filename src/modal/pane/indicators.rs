use crate::screen::dashboard::pane::{self, Message};
use crate::style::{self, Icon, icon_text};
use crate::widget::{column_drag, dragger_row};

use data::chart::indicator::Indicator;
use iced::{
    Element, Length, padding,
    widget::{button, column, container, horizontal_space, pane_grid, row, text},
};

pub fn view<'a, I: Indicator>(
    pane: pane_grid::Pane,
    state: &'a pane::State,
    selected: &[I],
) -> Element<'a, Message> {
    let market_type = state.settings.ticker_info.map(|info| info.market_type());

    let build_indicators = |allows_drag: bool| -> Element<'a, Message> {
        if let Some(market) = market_type {
            let indicator_row_elem_fn = |indicator: &I, is_selected_indicator: bool| {
                let content = if is_selected_indicator {
                    row![
                        text(indicator.to_string()),
                        horizontal_space(),
                        container(icon_text(Icon::Checkmark, 12)),
                    ]
                    .width(Length::Fill)
                } else {
                    row![text(indicator.to_string())].width(Length::Fill)
                };

                button(content)
                    .on_press(Message::ToggleIndicator(pane, indicator.to_string()))
                    .width(Length::Fill)
                    .style(move |theme, status| {
                        style::button::modifier(theme, status, is_selected_indicator)
                    })
                    .into()
            };

            let mut base_row_elements: Vec<Element<_>> = vec![];

            for indicator in selected {
                base_row_elements.push(indicator_row_elem_fn(indicator, true));
            }

            for indicator in I::for_market(market) {
                if !selected.contains(indicator) {
                    base_row_elements.push(indicator_row_elem_fn(indicator, false));
                }
            }

            let reorderable = allows_drag && selected.len() >= 2;

            let all_indicator_elements: Vec<Element<_>> = base_row_elements
                .into_iter()
                .map(|base_content| dragger_row(base_content, reorderable))
                .collect();

            let indicators_list_content: Element<_> = if reorderable {
                let mut draggable_column = column_drag::Column::new()
                    .on_drag(move |event| Message::ReorderIndicator(pane, event))
                    .spacing(4);
                for element in all_indicator_elements {
                    draggable_column = draggable_column.push(element);
                }
                draggable_column.into()
            } else {
                iced::widget::Column::with_children(all_indicator_elements)
                    .spacing(4)
                    .into()
            };

            column![
                container(text("Indicators").size(14)).padding(padding::bottom(8)),
                indicators_list_content
            ]
            .spacing(4)
            .into()
        } else {
            column![].spacing(4).into()
        }
    };

    let content_allows_dragging = matches!(state.content, pane::Content::Kline(_, _));
    let content_row = build_indicators(content_allows_dragging);

    container(content_row)
        .max_width(200)
        .padding(16)
        .style(style::chart_modal)
        .into()
}
