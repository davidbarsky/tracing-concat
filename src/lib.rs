use chashmap::CHashMap;
use std::io;
use tracing::{
    span,
    subscriber::{self, Subscriber},
    Event, Id, Metadata,
};
use tracing_core::span::Current;
use tracing_subscriber::{
    fmt::format::{DefaultFields, Format, Full},
    layer::{Context, Layer},
};

mod store;
use store::Store;

pub struct TracingConcatLayer {
    inner: TracingConcat,
}

impl Default for TracingConcatLayer {
    fn default() -> Self {
        Self {
            inner: TracingConcat::default(),
        }
    }
}

pub struct TracingConcat<N = DefaultFields, E = Format<Full>, W = fn() -> io::Stdout> {
    fmt_fields: N,
    fmt_event: E,
    spans: Store,
    events: CHashMap<Id, Vec<&'static Metadata<'static>>>,
    make_writer: W,
}

impl<S: Subscriber> Layer<S> for TracingConcatLayer {
    fn register_callsite(&self, meta: &'static Metadata<'static>) -> subscriber::Interest {
        self.inner.register_callsite(meta)
    }

    fn new_span(&self, attrs: &span::Attributes<'_>, _: &Id, _: Context<S>) {
        self.inner.spans.new_span(attrs, &self.inner.fmt_fields);
    }

    fn on_record(&self, span: &Id, values: &span::Record<'_>, _: Context<S>) {
        self.inner
            .spans
            .record(span, values, &self.inner.fmt_fields)
    }

    // fn on_event(&self, event: &Event<'_>, ctx: Context<S>) {
    //     println!("ctx: {:?}", ctx.current_span());
    //     println!("Event: {:?}", event);
    // }

    fn enabled(&self, metadata: &Metadata, _: Context<S>) -> bool {
        self.inner.enabled(metadata)
    }

    fn on_enter(&self, id: &Id, _: Context<S>) {
        self.inner.spans.pop(id);
    }

    fn on_exit(&self, id: &Id, _: Context<S>) {
        self.inner.spans.get(id).unwrap();
    }

    fn on_close(&self, id: Id, ctx: Context<S>) {
        if let Some(span) = self.inner.spans.get(&id) {
            println!("Span: {:?}", span);
            println!("Events: {:?}", self.inner.events)
        }
    }
}

impl Default for TracingConcat {
    fn default() -> Self {
        Self {
            fmt_fields: DefaultFields::default(),
            fmt_event: Format::default(),
            make_writer: io::stdout,
            spans: Store::with_capacity(32),
            events: CHashMap::new(),
        }
    }
}

impl Subscriber for TracingConcat {
    fn register_callsite(&self, _meta: &Metadata<'_>) -> subscriber::Interest {
        subscriber::Interest::always()
    }

    fn new_span(&self, attrs: &span::Attributes<'_>) -> Id {
        self.spans.new_span(attrs, &self.fmt_fields)
    }

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {
        // ignored
    }

    fn record(&self, span: &Id, values: &span::Record<'_>) {
        self.spans.record(span, values, &self.fmt_fields)
    }

    fn event(&self, _: &Event<'_>) {}

    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn enter(&self, id: &Id) {
        self.spans.pop(id);
    }

    fn exit(&self, id: &Id) {
        self.spans.get(id).unwrap();
    }

    #[inline]
    fn clone_span(&self, id: &span::Id) -> span::Id {
        self.spans.clone_span(id)
    }

    #[inline]
    fn try_close(&self, id: span::Id) -> bool {
        self.spans.drop_span(id)
    }

    fn current_span(&self) -> Current {
        if let Some(id) = self.spans.current() {
            if let Some(meta) = self.spans.get(&id).map(|span| span.metadata()) {
                return Current::new(id, meta);
            }
        }
        Current::none()
    }
}
