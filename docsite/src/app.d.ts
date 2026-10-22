declare global {
  namespace App {
    interface PageData {
      nav: import('$lib/server/content').Group[]
      order: import('$lib/server/content').PageRef[]
    }
  }
}

export {}
